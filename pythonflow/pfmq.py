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


import logging
import pickle
import queue
import struct
import threading
import uuid

import zmq


LOGGER = logging.getLogger(__name__)
MAX_INT, = struct.unpack('>I', b'\xff' * 4)


class Base:
    """
    Base class for running a ZeroMQ event loop in a background thread with a PAIR channel for
    cancelling the background thread.

    Parameters
    ----------
    start : bool
        Whether to start the event loop as a background thread.
    """
    def __init__(self, start):
        self._thread = None
        self._cancel_address = f'inproc://{uuid.uuid4().hex}'
        self._cancel_parent = zmq.Context.instance().socket(zmq.PAIR)  # pylint: disable=E1101
        self._cancel_parent.bind(self._cancel_address)

        if start:
            self.run_async()

    @property
    def is_alive(self):
        """
        bool : Whether the background thread is alive.
        """
        return self._thread and self._thread.is_alive()

    def cancel(self, timeout=None):
        """
        Cancel the event loop running in a background thread.

        Parameters
        ----------
        timeout : float
            Timeout for joining the background thread.
        """
        if self.is_alive:
            self._cancel_parent.send_multipart([b''])
            self._thread.join(timeout)
            self._cancel_parent.close()
        else:
            raise RuntimeError('background thread is not running')

    def run_async(self):
        """
        Run the event loop in a background thread.
        """
        if not self.is_alive:
            self._thread = threading.Thread(target=self.run, daemon=True)
            self._thread.start()
        return self._thread

    def run(self):
        """
        Run the event loop.

        Notes
        -----
        This call is blocking.
        """
        raise NotImplementedError


class Task(Base):
    """
    A task that is executed remotely.

    Tasks send one or more batches of `fetches` and `contexts` to the load balancer bound to
    `address` using the format `identifier, request`. Upon each request, they wait for a message
    acknowledging the receipt of the request comprising a single, empty frame or a result in the
    format `identifier, result`. If all batches have been dispatched but not all results have been
    received, tasks send a single empty frame to poll for additional results.

    Parameters
    ----------
    requests : iterable
        An iterable of requests.
    address : str
        Address of a load balancer backend.
    dumps : callable
        Function to serialize messages.
    loads : callable
        Function to deserialize messages.
    start : bool
        Whether to start the event loop as a background thread.
    """
    def __init__(self, requests, address, dumps=None, loads=None, start=True):  # pylint: disable=too-many-arguments
        self.requests = requests
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads
        self.results = queue.Queue()

        super(Task, self).__init__(start)

    def run(self):  # pylint: disable=too-many-locals
        context = zmq.Context.instance()
        last_identifier = None
        cache = {}
        next_identifier = 0

        with context.socket(zmq.PAIR) as cancel, context.socket(zmq.REQ) as socket:  # pylint: disable=E1101
            cancel.connect(self._cancel_address)
            socket.connect(self.address)

            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            poller.register(cancel, zmq.POLLIN)

            LOGGER.debug('connected to %s', self.address)

            current_identifier = -1
            batches = iter(self.requests)

            while next_identifier != last_identifier:
                # Dispatch a new request
                try:
                    batch = next(batches)
                    current_identifier = (current_identifier + 1) % MAX_INT
                    packed_identifier = struct.pack('>I', current_identifier)
                    socket.send_multipart([packed_identifier, self.dumps(batch)])
                    LOGGER.debug('sent request with identifier %s', packed_identifier)
                # Poll for results if all requests have been dispatched
                except StopIteration:
                    if last_identifier is None:
                        last_identifier = current_identifier
                        LOGGER.debug('set last identifier to %s after exhausting iterator',
                                     last_identifier)
                    socket.send_multipart([b''])
                    LOGGER.debug('sent request with empty identifier')

                LOGGER.debug('polling...')
                sockets = dict(poller.poll())
                if sockets.get(socket) == zmq.POLLIN:
                    identifier, *result = socket.recv_multipart()
                    if not identifier:
                        LOGGER.debug('received dispatch notification')
                        continue

                    # Add completed results to the results queue
                    LOGGER.debug('received result for identifier %s', identifier)
                    identifier, = struct.unpack('>I', identifier)
                    cache[identifier] = self.loads(*result)
                    while True:
                        try:
                            self.results.put((next_identifier, cache.pop(next_identifier)))
                            if next_identifier == last_identifier:
                                self.results.put((None, None))
                                break
                            next_identifier = (next_identifier + 1) % MAX_INT
                        except KeyError:
                            break

                # Cancel the communication thread
                if sockets.get(cancel) == zmq.POLLIN:
                    LOGGER.debug('received cancel signal on %s', self._cancel_address)
                    break

        LOGGER.debug('exiting communication loop')

    def iter_results(self, timeout=None):
        """
        Iterate over the results.

        Parameters
        ----------
        timeout : float
            Timeout for getting results.
        """
        while True:
            identifier, result = self.results.get(timeout=timeout)
            if identifier is None:
                break
            yield result

    def __iter__(self):
        return self.iter_results()


class Worker(Base):
    """
    A worker for executing remote fetches.

    Workers send an empty message to the load balancer bound to `address` to sign up and notify the
    load balancer of their availability. They wait for incoming messages of the format
    `*header, request`, apply `target` to the deserialised `request` to obtain a `result`, and
    respond with a multi-part message of the format `*header, result`.

    Parameters
    ----------
    target : callable
        A function to process incoming messages.
    address : str
        Address of a load balancer backend.
    dumps : callable
        Function to serialize messages.
    loads : callable
        Function to deserialize messages.
    start : bool
        Whether to start the event loop as a background thread.
    """
    def __init__(self, target, address, dumps=None, loads=None, start=False):  # pylint: disable=too-many-arguments
        self.target = target
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads

        super(Worker, self).__init__(start)

    def run(self):
        context = zmq.Context.instance()

        with context.socket(zmq.REQ) as socket, context.socket(zmq.PAIR) as cancel:  # pylint: disable=E1101
            cancel.connect(self._cancel_address)
            socket.connect(self.address)
            LOGGER.debug('connected to %s', self.address)

            poller = zmq.Poller()
            poller.register(cancel, zmq.POLLIN)
            poller.register(socket, zmq.POLLIN)

            socket.send_multipart([b''])
            LOGGER.debug('sent sign-up message')

            while True:
                LOGGER.debug('polling...')
                sockets = dict(poller.poll())

                # Process messages
                if sockets.get(socket) == zmq.POLLIN:
                    client, _, identifier, *request = socket.recv_multipart()
                    LOGGER.debug('received request with identifier %s from %s', identifier, client)
                    result = self.dumps(self.target(self.loads(*request)))
                    socket.send_multipart([client, _, identifier, result])
                    LOGGER.debug('sent result with identifier %s to %s', identifier, client)

                # Cancel the communication thread
                if sockets.get(cancel) == zmq.POLLIN:
                    LOGGER.debug('received cancel signal on %s', self._cancel_address)
                    break

        LOGGER.debug('exiting communication loop')

    @classmethod
    def from_graph(cls, graph, *args, **kwargs):
        """
        Construct a worker from a computational graph.

        Parameters
        ----------
        graph : Graph
            A computational graph.
        """
        def _target(request):
            if 'context' in request:
                return graph(request['fetches'], request['context'])
            elif 'contexts' in request:
                return [graph(request['fetches'], context) for context in request['contexts']]
            raise KeyError
        return cls(_target, *args, **kwargs)


class MessageBroker(Base):
    """
    Message broker for tasks that are executed remotely.

    Parameters
    ----------
    backend_address : str
        Address to which workers can connect.
    frontend_address : str
        Address to which tasks can connect (defaults to a random in-process communication channel).
    start : bool
        Whether to start the event loop as a background thread.
    """
    def __init__(self, backend_address, frontend_address=None, start=False):
        self.backend_address = backend_address
        self.frontend_address = frontend_address or f'inproc://{uuid.uuid4().hex}'
        super(MessageBroker, self).__init__(start)

    def run(self):  # pylint: disable=too-many-statements,too-many-locals
        context = zmq.Context.instance()
        workers = set()
        cache = {}

        # pylint: disable=E1101
        with context.socket(zmq.ROUTER) as frontend, context.socket(zmq.ROUTER) as backend, \
            context.socket(zmq.PAIR) as cancel:
        # pylint: enable=E1101

            cancel.connect(self._cancel_address)
            frontend.bind(self.frontend_address)
            LOGGER.debug('bound frontend to %s', self.frontend_address)
            backend.bind(self.backend_address)
            LOGGER.debug('bound backend to %s', self.backend_address)

            backend_poller = zmq.Poller()
            backend_poller.register(backend, zmq.POLLIN)
            backend_poller.register(cancel, zmq.POLLIN)

            poller = zmq.Poller()
            poller.register(frontend, zmq.POLLIN)
            poller.register(backend, zmq.POLLIN)
            poller.register(cancel, zmq.POLLIN)

            while True:
                # Only listen to the backend if no workers are available
                if workers:
                    LOGGER.debug('polling frontend and backend...')
                    sockets = dict(poller.poll())
                else:
                    LOGGER.debug('polling backend...')
                    sockets = dict(backend_poller.poll())

                # Receive results or sign-up messages from the backend
                if sockets.get(backend) == zmq.POLLIN:
                    worker, _, client, *message = backend.recv_multipart()
                    workers.add(worker)

                    if client:
                        _, identifier, result = message
                        cache.setdefault(client, []).append((identifier, result))
                        LOGGER.debug('received result with identifier %s from %s for %s',
                                     identifier, worker, client)
                    else:
                        LOGGER.debug('received sign-up message from %s', worker)

                # Receive requests from the frontend and forward to the workers or return results
                if sockets.get(frontend) == zmq.POLLIN:
                    client, _, identifier, *request = frontend.recv_multipart()
                    LOGGER.debug('received request with identifier %s from %s', identifier, client)

                    if identifier:
                        worker = workers.pop()
                        backend.send_multipart([worker, _, client, _, identifier, *request])
                        LOGGER.debug('sent request with identifier %s from %s to %s',
                                     identifier, client, worker)

                    try:
                        identifier, result = cache[client].pop(0)
                        frontend.send_multipart([client, _, identifier, result])
                        LOGGER.debug('sent result with identifier %s from %s to %s',
                                     identifier, worker, client)
                    except (KeyError, IndexError):
                        # Only send a dispatch notification if the task had an identifier
                        if identifier:
                            frontend.send_multipart([client, _, _])
                            LOGGER.debug('notified %s of request dispatch', client)

                # Cancel the communication thread
                if sockets.get(cancel) == zmq.POLLIN:
                    LOGGER.debug('received cancel signal on %s', self._cancel_address)
                    break

        LOGGER.debug('exiting communication loop')

    def imap(self, requests, **kwargs):
        """
        Convenience method for applying a target to requests remotely.
        """
        return Task(requests, self.frontend_address, **kwargs)

    def apply(self, request, **kwargs):
        """
        Convenience method for applying a target to a request remotely.
        """
        task = self.imap([request], start=False, **kwargs)
        task.run()
        _, result = task.results.get()
        return result
