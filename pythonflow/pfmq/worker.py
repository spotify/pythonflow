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
import sys
import traceback
import uuid

import zmq

from ._base import Base, TopicError


LOGGER = logging.getLogger(__name__)


class Worker(Base):
    """
    A worker for executing remote fetches.

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
    max_retries : int
        Number of retry attempts or `None` for indefinite retries. Defaults to `None` for workers
        because they will time out if the load balancer does not provide any work.
    topic : bytes or None
        Topic for the communication. Only sockets with the same topic are allowed to communicate
        with one another to avoid unexpected communication with unintended remote graphs.
    """
    def __init__(self, target, address, dumps=None, loads=None, start=False, timeout=10,  # pylint: disable=too-many-arguments
                 max_retries=None, topic=None):
        self.target = target
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads
        self.max_retries = max_retries
        self.timeout = timeout

        super(Worker, self).__init__(start, topic)

    def run(self):  # pylint: disable=too-many-locals,too-many-statements
        context = zmq.Context.instance()
        # Use a specific identity for the worker such that reconnects don't change it.
        identity = uuid.uuid4().bytes

        num_retries = 0
        while self.max_retries is None or num_retries < self.max_retries:
            with context.socket(zmq.REQ) as socket, context.socket(zmq.PAIR) as cancel:  # pylint: disable=E1101
                cancel.connect(self._cancel_address)
                socket.setsockopt(zmq.IDENTITY, identity)  # pylint: disable=E1101
                socket.connect(self.address)
                LOGGER.debug('connected to %s', self.address)

                poller = zmq.Poller()
                poller.register(cancel, zmq.POLLIN)
                poller.register(socket, zmq.POLLIN)

                socket.send_multipart([self.STATUS['sign_up'], self.topic])
                LOGGER.debug('sent sign-up message')

                while True:
                    LOGGER.debug('polling...')
                    sockets = dict(poller.poll(1000 * self.timeout))

                    if not sockets:
                        num_retries += 1
                        LOGGER.info('time out #%d for %s after %.3f seconds', num_retries,
                                    self.address, self.timeout)
                        break

                    num_retries = 0

                    # Cancel the communication thread
                    if sockets.get(cancel) == zmq.POLLIN:
                        LOGGER.debug('received cancel signal on %s', self._cancel_address)
                        return

                    # Process messages
                    if sockets.get(socket) == zmq.POLLIN:
                        status, *payload = socket.recv_multipart()

                        if status == self.STATUS['request']:
                            client, identifier, request = payload
                            LOGGER.debug('received REQUEST with identifier %d from %s',
                                         int.from_bytes(identifier, 'little'), client.hex())

                            try:
                                response = self.target(self.loads(request))
                                status = self.STATUS['response']
                            except Exception:  # pylint: disable=broad-except
                                etype, value, tb = sys.exc_info()  # pylint: disable=invalid-name
                                response = (
                                    value,
                                    "".join(traceback.format_exception(etype, value, tb))
                                )
                                status = self.STATUS['response_error']
                                LOGGER.exception(
                                    "failed to process REQUEST with identifier %d from %s",
                                    int.from_bytes(identifier, 'little'), client.hex()
                                )

                            try:
                                response = self.dumps(response)
                            except Exception:  # pylint: disable=broad-except
                                LOGGER.exception(
                                    "failed to serialise RESPONSE with identifier %d for %s",
                                    int.from_bytes(identifier, 'little'), client.hex()
                                )
                                response = b''
                                status = self.STATUS['serialization_error']

                            socket.send_multipart([
                                status, self.topic, client, identifier, response
                            ])
                            LOGGER.debug(
                                'sent RESPONSE with identifier %s to %s with status %s',
                                int.from_bytes(identifier, 'little'), client.hex(),
                                self.STATUS[status]
                            )
                        elif status == self.STATUS['topic_error']:
                            topic, = payload
                            raise TopicError(f"broker expected topic {topic} but got {self.topic}")
                        else:  # pragma: no cover
                            raise KeyError(status)

        LOGGER.error("maximum number of retries (%d) for %s exceeded", self.max_retries,
                     self.address)

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
