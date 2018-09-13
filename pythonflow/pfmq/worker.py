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
import sys
import traceback
import uuid

import zmq

from ._base import Base, Code

LOGGER = logging.getLogger(__name__)


class Worker(Base):  # pylint: disable=too-many-instance-attributes
    """
    Worker for executing remote tasks.

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
    timeout : float
        Number of seconds before a request times out.
    num_retries : int
        Number of retry attempts or `None` for indefinite retries. Defaults to `None` for workers
        because they will time out if the load balancer does not provide any work.
    prefetch : int
        Number of requests to prefetch from the broker.
    """
    def __init__(self, target, address, dumps=None, loads=None, start=True, timeout=10,  # pylint: disable=too-many-arguments
                 num_retries=None, prefetch=1):
        self.target = target
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads
        self.num_retries = num_retries
        self.timeout = timeout
        self.prefetch = prefetch

        self._result_available_address = f'inproc://{uuid.uuid4().hex}'
        self.requests = queue.Queue(self.prefetch)

        super(Worker, self).__init__(start)

    def run(self):
        num_tries = 0
        identity = uuid.uuid4().bytes
        timeout = None if self.timeout is None else 1000 * self.timeout

        # pylint: disable=no-member
        with zmq.Context.instance().socket(zmq.PAIR) as cancel, \
                zmq.Context.instance().socket(zmq.PAIR) as result_available:
        # pylint: enable=no-member
            cancel.bind(self._cancel_address)
            LOGGER.debug("bound cancel socket to %s", self._cancel_address)
            result_available.bind(self._result_available_address)
            LOGGER.debug("bound result_available socket to %s", self._result_available_address)

            while self.num_retries is None or num_tries < self.num_retries:
                with zmq.Context.instance().socket(zmq.DEALER) as socket:  # pylint: disable=no-member

                    socket.setsockopt(zmq.IDENTITY, identity)  # pylint: disable=no-member
                    socket.connect(self.address)

                    LOGGER.debug("connected to broker backend on %s with identity %s", self.address,
                                 identity)

                    poller = zmq.Poller()
                    poller.register(cancel, zmq.POLLIN)
                    poller.register(result_available, zmq.POLLIN)
                    poller.register(socket, zmq.POLLIN)

                    socket.send_multipart([Code.SIGN_UP.value])

                    while True:
                        events = dict(poller.poll(timeout))

                        if not events:
                            num_tries += 1
                            LOGGER.info("time out #%d after %.3f seconds", num_tries, self.timeout)
                            break

                        if events.get(cancel) == zmq.POLLIN:
                            LOGGER.info("received CANCEL signal on %s", self._cancel_address)
                            socket.send_multipart([Code.SIGN_OFF.value])
                            # Empty the queue and put a cancel signal
                            while True:
                                try:
                                    self.requests.get_nowait()
                                except queue.Empty:
                                    break
                            self.requests.put([Code.CANCEL])
                            return

                        if events.get(result_available) == zmq.POLLIN:
                            socket.send_multipart(result_available.recv_multipart())
                            LOGGER.info("sent response to broker (%d/%d requests in queue)",
                                        self.requests.qsize(), self.prefetch)

                        if events.get(socket) == zmq.POLLIN:
                            # Put the request on the queue and send a dispatch message
                            code, client, *parts = socket.recv_multipart()
                            code = Code(code)
                            if code == Code.REQUEST:
                                self.requests.put([code, client, *parts])
                                socket.send_multipart([Code.SIGN_UP.value])
                                LOGGER.info("received %s from %s (%d/%d requests in queue)",
                                            code, client, self.requests.qsize(), self.prefetch)
                            else:  # pragma: no cover
                                raise ValueError(code)

            LOGGER.error("time out after %d retries", self.num_retries)

    def process_requests(self):
        """
        Process incoming requests.

        Notes
        -----
        This call is blocking.
        """
        if not self.is_alive:
            raise RuntimeError("the communication thread must be running to process requests")

        with zmq.Context.instance().socket(zmq.PAIR) as result_available:  # pylint: disable=no-member
            result_available.connect(self._result_available_address)

            while True:
                code, *parts = self.requests.get()

                if code == Code.REQUEST:
                    try:
                        client, identifier, request = parts
                        try:
                            result = self.target(self.loads(request))
                        except Exception:  # pylint: disable=broad-except
                            etype, value, tb = sys.exc_info()  # pylint: disable=invalid-name
                            response = value, "".join(traceback.format_exception(etype, value, tb))
                            result_available.send_multipart(
                                [Code.ERROR.value, client, identifier, self.dumps(response)],
                                flags=zmq.NOBLOCK)  # pylint: disable=no-member
                            LOGGER.exception("failed to process request with identifier %s from %s",
                                             identifier, client)
                            continue

                        try:
                            result = self.dumps(result)
                        except Exception:  # pylint: disable=broad-except
                            result_available.send_multipart(
                                [Code.SERIALIZATION_ERROR.value, client, identifier],
                                flags=zmq.NOBLOCK)  # pylint: disable=no-member
                            LOGGER.exception("failed to serialise result for request with "
                                             "identifier %s from %s", identifier, client)
                            continue

                        result_available.send_multipart(
                            [Code.RESULT.value, client, identifier, result], flags=zmq.NOBLOCK)  # pylint: disable=no-member
                        LOGGER.debug("processed request with identifier %s for %s", identifier,
                                     client)
                    except zmq.error.Again:
                        return
                elif code == Code.CANCEL:
                    return
                else:  # pragma: no cover
                    raise ValueError(code)

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
            if 'contexts' in request:
                return [graph(request['fetches'], context) for context in request['contexts']]
            raise KeyError("`context` or `contexts` must be in the request")

        return cls(_target, *args, **kwargs)
