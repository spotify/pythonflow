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
import uuid
import zmq

from ._base import Base
from .task import Task, apply


LOGGER = logging.getLogger(__name__)


class Broker(Base):
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
    topic : bytes or None
        Topic for the communication. Only sockets with the same topic are allowed to communicate
        with one another to avoid unexpected communication with unintended remote graphs.
    """
    def __init__(self, backend_address, frontend_address=None, start=False, topic=None):
        self.backend_address = backend_address
        self.frontend_address = frontend_address or f'inproc://{uuid.uuid4().hex}'
        super(Broker, self).__init__(start, topic)

    def run(self):  # pylint: disable=too-many-statements,too-many-locals,too-many-branches
        context = zmq.Context.instance()
        workers = set()
        clients = set()
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

                # Cancel the communication thread
                if sockets.get(cancel) == zmq.POLLIN:
                    LOGGER.debug('received CANCEL signal on %s', self._cancel_address)
                    break

                # Receive responses or sign-up messages from the backend
                if sockets.get(backend) == zmq.POLLIN:
                    worker, _, status, topic, *payload = backend.recv_multipart()

                    if topic != self.topic:
                        LOGGER.warning('received RESPONSE/SIGN-UP with topic %s from %s (expected '
                                       '%s)', topic, worker.hex(), self.topic)
                        backend.send_multipart([worker, _, self.STATUS['topic_error'], self.topic])
                    else:
                        workers.add(worker)

                        if status in (self.STATUS['response'], self.STATUS['response_error'],
                                      self.STATUS['serialization_error']):
                            client, identifier, response = payload
                            LOGGER.debug('received RESPONSE with identifier %s from %s for %s with '
                                         'status %s', int.from_bytes(identifier, 'little'),
                                         worker.hex(), client.hex(), self.STATUS[status])
                            # Try to forward the message to a waiting client
                            try:
                                clients.remove(client)
                                self._forward_response(frontend, client, identifier, status,
                                                       response)
                            # Add it to the cache otherwise
                            except KeyError:
                                cache.setdefault(client, []).append((identifier, status, response))
                        elif status == self.STATUS['sign_up']:
                            LOGGER.debug('received SIGN-UP message from %s; now %d workers',
                                         worker.hex(), len(workers))
                        else:  # pragma: no cover
                            raise KeyError(status)

                # Receive requests from the frontend, forward to the workers, and return responses
                if sockets.get(frontend) == zmq.POLLIN:
                    client, _, status, topic, *payload = frontend.recv_multipart()

                    if topic != self.topic:
                        LOGGER.warning('received REQUEST with topic %s from %s (expected %s)',
                                       topic, client.hex(), self.topic)
                        frontend.send_multipart([client, _, self.STATUS['topic_error'], self.topic])
                    else:
                        if status == self.STATUS['request']:
                            worker = workers.pop()
                            backend.send_multipart([worker, _, self.STATUS['request'], client,
                                                    *payload])
                            LOGGER.debug('forwarded REQUEST with identifier %s from %s to %s',
                                         int.from_bytes(payload[0], 'little'), client.hex(),
                                         worker.hex())

                        try:
                            self._forward_response(frontend, client, *cache[client].pop(0))
                        except (KeyError, IndexError):
                            # Send a dispatch notification if the task sent a new message
                            if status == self.STATUS['request']:
                                frontend.send_multipart([client, _, self.STATUS['dispatch']])
                                LOGGER.debug('notified %s of REQUEST dispatch', client.hex())
                            # Add the task to the list of tasks waiting for responses otherwise
                            else:
                                clients.add(client)

        LOGGER.debug('exiting communication loop')

    @classmethod
    def _forward_response(cls, frontend, client, identifier, status, response):  # pylint: disable=too-many-arguments
        frontend.send_multipart([client, b'', status, identifier, response])
        LOGGER.debug('forwarded RESPONSE with identifier %s to %s with status %s',
                     int.from_bytes(identifier, 'little'), client, cls.STATUS[status])

    def imap(self, requests, **kwargs):
        """
        Process a sequence of requests remotely.

        Parameters
        ----------
        requsests : iterable
            Sequence of requests to process.

        Returns
        -------
        task : Task
            Remote task that can be iterated over.
        """
        if not self.is_alive:
            raise RuntimeError("broker is not running")
        kwargs.setdefault('topic', self.topic)

        return Task(requests, self.frontend_address, **kwargs)

    def apply(self, request, **kwargs):
        """
        Process a request remotely.

        Parameters
        ----------
        request : object
            Request to process.

        Returns
        -------
        ressult : object
            Result of remotely-processed request.
        """
        if not self.is_alive:
            raise RuntimeError("broker is not running")
        kwargs.setdefault('topic', self.topic)

        return apply(request, self.frontend_address, **kwargs)
