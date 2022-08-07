

# Copyright 2018 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import logging
import pickle
import queue
import time
import uuid

import zmq

from ._base import Base


LOGGER = logging.getLogger(__name__)


class SerializationError(RuntimeError):
    """
    Error serialising a remote result.
    """


class Task(Base):
    """
    A task that is executed remotely.

    Tasks send one or more batches of `fetches` and `contexts` to the broker bound to `address`
    using the format `identifier, request`. Upon each request, they wait for a message acknowledging
    the receipt of the request comprising a single, empty frame or a result in the format
    `identifier, result`. If all batches have been dispatched but not all results have been
    received, tasks send a single empty frame to poll for additional results.

    Parameters
    ----------
    requests : iterable
        An iterable of requests.
    address : str
        Address of a broker frontend.
    dumps : callable
        Function to serialize messages.
    loads : callable
        Function to deserialize messages.
    start : bool
        Whether to start the event loop as a background thread.
    timeout : float
        Number of seconds before a request times out.
    max_retries : int
        Number of retry attempts.
    cache_size : int
        Maximum number of results to cache. If `cache_size <= 0`, the cache is infinite which can
        lead to the exhaustion of memory resources.
    """
    def __init__(self, requests, address, dumps=None, loads=None, start=True, timeout=10,
                 max_retries=3, max_results=1024):
        self.requests = requests
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads
        self.max_results = max_results
        self.results = queue.Queue(self.max_results)
        self.timeout = timeout
        self.max_retries = max_retries

        super(Task, self).__init__(start)

    def run(self):
        context = zmq.Context.instance()
        num_retries = 0
        identity = uuid.uuid4().bytes
        pending = collections.OrderedDict()
        requests = iter(enumerate(self.requests, 0))
        current_identifier = last_identifier = None
        next_identifier = 0
        cache = {}

        while True:
            with context.socket(zmq.PAIR) as cancel, context.socket(zmq.REQ) as socket:
                cancel.connect(self._cancel_address)
                socket.setsockopt(zmq.IDENTITY, identity)
                socket.connect(self.address)

                poller = zmq.Poller()
                poller.register(socket, zmq.POLLIN)
                poller.register(cancel, zmq.POLLIN)

                LOGGER.debug('connected %s to %s', identity, self.address)

                while True:
                    LOGGER.debug("%d pending messages", len(pending))
                    # See whether we can find a request that has timed out
                    identifier = message = None
                    for candidate, item in pending.items():
                        delta = time.time() - item['time']
                        if delta > self.timeout:
                            identifier = candidate
                            LOGGER.info('request with identifier %d timed out', identifier)
                            message = item['message']
                        # We only need to check the first message
                        break

                    # Get a new message
                    if identifier is None:
                        try:
                            identifier, request = next(requests)
                            num_bytes = max((identifier.bit_length() + 7) // 8, 1)
                            message = [
                                identifier.to_bytes(num_bytes, 'little'),
                                self.dumps(request)
                            ]
                            # Store the most recent identifier
                            current_identifier = identifier
                            LOGGER.debug('new request with identifier %d', identifier)
                        except StopIteration:
                            last_identifier = last_identifier or current_identifier
                            identifier = None
                            message = [b'']
                            LOGGER.debug('no more requests; waiting for responses')

                    socket.send_multipart(message)
                    LOGGER.debug('sent REQUEST with identifier %s: %s', identifier, message)

                    # Add to the list of pending requests
                    if identifier is not None:
                        pending[identifier] = {
                            'message': message,
                            'time': time.time()
                        }
                    del identifier

                    # Retrieve a response
                    LOGGER.debug('polling...')
                    sockets = dict(poller.poll(1000 * self.timeout))

                    # Communication timed out
                    if not sockets:
                        num_retries += 1
                        LOGGER.info('time out #%d for %s after %.3f seconds', num_retries,
                                    self.address, self.timeout)
                        if self.max_retries and num_retries >= self.max_retries:
                            # The communication failed
                            message = "maximum number of retries (%d) for %s exceeded" % \
                                (self.max_retries, self.address)
                            LOGGER.error(message)
                            self.results.put(('timeout', TimeoutError(message)))
                            return
                        break  # pragma: no cover

                    # Reset the retry counter
                    num_retries = 0

                    # Cancel the communication thread
                    if sockets.get(cancel) == zmq.POLLIN:
                        LOGGER.debug('received CANCEL signal on %s', self._cancel_address)
                        return

                    if sockets.get(socket) == zmq.POLLIN:
                        identifier, *response = socket.recv_multipart()
                        if not identifier:
                            LOGGER.debug('received dispatch notification')
                            continue

                        # Decode the identifier and remove the corresponding request from `pending`
                        identifier = int.from_bytes(identifier, 'little')
                        LOGGER.debug('received RESPONSE for identifier %d (next: %d, end: %s)',
                                     identifier, next_identifier, last_identifier)
                        pending.pop(identifier, None)

                        # Drop the message if it is outdated
                        if identifier < next_identifier:  # pragma: no cover
                            LOGGER.debug('dropped RESPONSE with identifier %d (next: %d)',
                                         identifier, next_identifier)
                            continue

                        # Add the message to the cache
                        cache[identifier] = response
                        while True:
                            try:
                                status, response = cache.pop(next_identifier)
                                status = self.STATUS[status]
                                self.results.put(
                                    (status, identifier if status == 'serialization_error' else
                                     self.loads(response))
                                )

                                if next_identifier == last_identifier:
                                    self.results.put(('end', None))
                                    return
                                next_identifier += 1
                            except KeyError:
                                break

    def iter_results(self, timeout=None):
        """
        Iterate over the results.

        Parameters
        ----------
        timeout : float
            Timeout for getting results.
        """
        while True:
            status, result = self.results.get(timeout=timeout)
            if status == 'ok':
                yield result
            elif status in 'error':
                value, tb = result
                LOGGER.error(tb)
                raise value
            elif status == 'timeout':
                raise result
            elif status == 'end':
                return
            elif status == 'serialization_error':
                raise SerializationError(
                    "failed to serialize result for request with identifier %s" % result
                )
            else:
                raise KeyError(status)  # pragma: no cover

    def __iter__(self):
        return self.iter_results()


def apply(request, frontend_address, **kwargs):
    """
    Process a request remotely.

    Parameters
    ----------
    request : object
        Request to process.
    frontend_address : str
        Address of the broker frontend.

    Returns
    -------
    ressult : object
        Result of remotely-processed request.
    """
    task = Task([request], frontend_address, start=False, **kwargs)
    task.run()
    for result in task.iter_results(timeout=0):
        return result
