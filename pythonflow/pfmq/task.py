# pylint: disable=missing-docstring
# pylint: enable=missing-docstring
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

from ._base import BYTEORDER, Base, Code

LOGGER = logging.getLogger(__name__)


class SerializationError(RuntimeError):
    """
    Raised if the result cannot be serialised.
    """


class RemoteError(RuntimeError):
    """
    Raised if a remote execution failed.
    """


class Task(Base):  # pylint: disable=too-many-instance-attributes
    """
    A task that is executed remotely.

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
    num_retries : int
        Number of retry attempts.
    cache_size : int
        Maximum number of results to cache. If `cache_size <= 0`, the cache is infinite which can
        lead to the exhaustion of memory resources.
    ordered : bool
        Whether results are yielded in order.
    """
    def __init__(self, requests, address, dumps=None, loads=None, start=True, timeout=10,  # pylint: disable=too-many-arguments
                 num_retries=3, cache_size=64, ordered=True):
        self.requests = requests
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads
        self.cache_size = cache_size
        self.results = queue.Queue(self.cache_size)
        self.timeout = timeout
        self.num_retries = num_retries
        self.ordered = ordered

        super(Task, self).__init__(start)

    def run(self):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        identity = uuid.uuid4().bytes
        pending = collections.OrderedDict()
        exhausted = False
        next_identifier = 0
        requests = iter(enumerate(self.requests))
        cache = {}

        with zmq.Context.instance().socket(zmq.PAIR) as cancel:  # pylint: disable=no-member
            cancel.bind(self._cancel_address)
            LOGGER.debug("bound cancel socket to %s", self._cancel_address)

            while True:
                # pylint: disable=no-member
                with zmq.Context.instance().socket(zmq.DEALER) as socket:
                    socket.setsockopt(zmq.IDENTITY, identity)
                # pylint: enable=no-member
                    socket.connect(self.address)
                    LOGGER.debug("connected to broker frontend on %s with identity %s",
                                 self.address, identity)

                    poller = zmq.Poller()
                    poller.register(socket, zmq.POLLIN)
                    poller.register(cancel, zmq.POLLIN)

                    while True:
                        message = None
                        timeout = self.timeout

                        # Get the first pending message that has timed out, if any
                        for identifier, item in pending.items():
                            delta = time.time() - item['time']
                            # Requeue the message if it has timed out
                            if delta > self.timeout:
                                num_tries = item['num_tries']
                                if num_tries >= self.num_retries:
                                    message = f"message with identifier {identifier} timed out " \
                                        f"after {self.num_retries} retries"
                                    self.results.put((Code.TIMEOUT, identifier,
                                                      TimeoutError(message)))
                                    LOGGER.error(message)
                                    return
                                message = item['message']
                                del pending[identifier]
                                LOGGER.info("retry #%d for request with identifier %d", num_tries,
                                            identifier)
                            # Reduce the timeout if it has not yet timed out
                            else:
                                timeout = self.timeout - delta
                            # We only need to check the first message because `pending` is ordered
                            break

                        # Get the next message if there are no pending ones
                        if message is None and not exhausted:
                            try:
                                identifier, request = next(requests)
                                num_bytes = max((identifier.bit_length() + 7) // 8, 1)
                                message = [
                                    Code.REQUEST.value,
                                    identifier.to_bytes(num_bytes, BYTEORDER),
                                    self.dumps(request)
                                ]
                                num_tries = 0
                                LOGGER.info("queuing request with identifier %d", identifier)
                            except StopIteration:
                                LOGGER.debug("exhausted iterator of requests")
                                exhausted = True
                                if not pending:
                                    self.results.put(None)
                                    LOGGER.debug("enqueuing end marker")
                                    return

                        # Send a message
                        if message is not None:
                            socket.send_multipart(message)
                            pending[identifier] = {
                                'time': time.time(),
                                'num_tries': num_tries + 1,
                                'message': message
                            }

                        events = dict(poller.poll(1000 * timeout))

                        if not events:
                            break

                        if events.get(cancel) == zmq.POLLIN:
                            self.results.put((False, RuntimeError("task cancelled")))
                            LOGGER.info("received CANCEL signal on %s", self._cancel_address)
                            return

                        # Receive results
                        if events.get(socket) == zmq.POLLIN:
                            code, identifier, *parts = socket.recv_multipart()
                            identifier = int.from_bytes(identifier, BYTEORDER)
                            code = Code(code)
                            LOGGER.info("received %s for request with identifier %d", code,
                                        identifier)

                            if code in (Code.RESULT, Code.ERROR, Code.SERIALIZATION_ERROR):
                                cache[identifier] = (code, identifier, *parts)
                                pending.pop(identifier)
                            elif code == Code.ENQUEUED:
                                continue
                            else:  # pragma: no cover
                                raise ValueError(code)

                        # Put items on the results queue in the correct order
                        while True:
                            try:
                                if self.ordered:
                                    value = cache.pop(next_identifier)
                                    next_identifier += 1
                                else:
                                    _, value = cache.popitem()
                                self.results.put(value)
                            except KeyError:
                                break

                        # We've received results for all messages
                        if exhausted and not pending:
                            self.results.put(None)
                            LOGGER.debug("enqueuing end marker")
                            return

    def iter_results(self, timeout=None):
        """
        Iterate over results of the task.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        """
        while True:
            item = self.results.get(timeout=timeout)
            if item is None:
                return
            code, identifier, *parts = item
            if code == Code.RESULT:
                yield self.loads(*parts)
            elif code == Code.ERROR:
                ex, traceback = self.loads(*parts)
                raise ex from RemoteError(traceback)
            elif code == Code.SERIALIZATION_ERROR:
                raise SerializationError(identifier)
            elif code == Code.TIMEOUT:
                raise parts[0]
            else:  # pragma: no cover
                raise ValueError(code)

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
