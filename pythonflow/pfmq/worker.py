

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

from ._base import Base


LOGGER = logging.getLogger(__name__)


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
    timeout : float
        Number of seconds before a request times out.
    max_retries : int
        Number of retry attempts or `None` for indefinite retries. Defaults to `None` for workers
        because they will time out if the load balancer does not provide any work.
    """
    def __init__(self, target, address, dumps=None, loads=None, start=False, timeout=10,
                 max_retries=None):
        self.target = target
        self.address = address
        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads
        self.max_retries = max_retries
        self.timeout = timeout

        super(Worker, self).__init__(start)

    def run(self):
        context = zmq.Context.instance()
        # Use a specific identity for the worker such that reconnects don't change it.
        identity = uuid.uuid4().bytes

        num_retries = 0
        while self.max_retries is None or num_retries < self.max_retries:
            with context.socket(zmq.REQ) as socket, context.socket(zmq.PAIR) as cancel:
                cancel.connect(self._cancel_address)
                socket.setsockopt(zmq.IDENTITY, identity)
                socket.connect(self.address)
                LOGGER.debug('connected to %s', self.address)

                poller = zmq.Poller()
                poller.register(cancel, zmq.POLLIN)
                poller.register(socket, zmq.POLLIN)

                socket.send_multipart([b''])
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
                        client, _, identifier, *request = socket.recv_multipart()
                        LOGGER.debug('received REQUEST with identifier %d from %s',
                                     int.from_bytes(identifier, 'little'), client.hex())

                        try:
                            response = self.target(self.loads(*request))
                            status = self.STATUS['ok']
                        except Exception:
                            etype, value, tb = sys.exc_info()
                            response = value, "".join(traceback.format_exception(etype, value, tb))
                            status = self.STATUS['error']
                            LOGGER.exception("failed to process REQUEST with identifier %d from %s",
                                             int.from_bytes(identifier, 'little'), client.hex())

                        try:
                            response = self.dumps(response)
                        except Exception:
                            LOGGER.exception(
                                "failed to serialise RESPONSE with identifier %d for %s",
                                int.from_bytes(identifier, 'little'), client.hex()
                            )
                            response = b""
                            status = self.STATUS['serialization_error']

                        socket.send_multipart([client, b'', identifier, status, response])
                        LOGGER.debug(
                            'sent RESPONSE with identifier %s to %s with status %s',
                            int.from_bytes(identifier, 'little'), client.hex(), self.STATUS[status]
                        )

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
