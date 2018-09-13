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

from ._base import Base, Code
from .task import Task, apply

LOGGER = logging.getLogger(__name__)


class Broker(Base):
    """
    Broker to distribute requests to workers and collate results.

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
        super(Broker, self).__init__(start)

    def run(self):
        workers = set()
        requests = []
        results = []

        # pylint: disable=no-member
        with zmq.Context.instance().socket(zmq.PAIR) as cancel, \
                zmq.Context.instance().socket(zmq.ROUTER) as frontend, \
                zmq.Context.instance().socket(zmq.ROUTER) as backend:
        # pylint: enable=no-member

            cancel.bind(self._cancel_address)
            LOGGER.debug("bound cancel socket to %s", self._cancel_address)
            frontend.bind(self.frontend_address)
            LOGGER.info("bound frontend to %s", self.frontend_address)
            backend.bind(self.backend_address)
            LOGGER.info("bound backend to %s", self.backend_address)


            poller = zmq.Poller()
            poller.register(cancel, zmq.POLLIN)
            poller.register(frontend, zmq.POLLIN)
            poller.register(backend, zmq.POLLIN)

            while True:
                events = dict(poller.poll())

                if events.get(cancel) == zmq.POLLIN:
                    LOGGER.info("received CANCEL signal on %s", self._cancel_address)
                    return

                # Handle incoming results or sign-up messages
                if events.get(backend) == zmq.POLLIN:
                    worker, code, *parts = backend.recv_multipart()
                    code = Code(code)
                    if code in (Code.RESULT, Code.ERROR, Code.SERIALIZATION_ERROR):
                        client, identifier, *parts = parts
                        results.append([client, code, identifier, *parts])
                        LOGGER.info("received %s from worker %s for client %s with identifier %s",
                                    code, worker, client, identifier)
                    elif code == Code.SIGN_UP:
                        workers.add(worker)
                        LOGGER.info("received %s from worker %s (now %d workers)", code, worker,
                                    len(workers))
                    elif code == Code.SIGN_OFF:
                        workers.discard(worker)
                        LOGGER.info("received %s from worker %s (now %d workers)", code, worker,
                                    len(workers))
                    else:  # pragma: no cover
                        raise ValueError(code)

                # Handle incoming requests
                if events.get(frontend) == zmq.POLLIN:
                    client, code, *parts = frontend.recv_multipart()
                    code = Code(code)
                    if code == Code.REQUEST:
                        LOGGER.info("received %s from client %s", code, client)
                        requests.append([client, code, *parts])
                    else:  # pragma: no cover
                        raise ValueError(code)

                # Dispatch all requests to workers
                while workers and requests:
                    worker = workers.pop()
                    client, code, identifier, *parts = requests.pop(0)
                    backend.send_multipart([worker, code.value, client, identifier, *parts])
                    LOGGER.info("sent %s to worker %s", Code.REQUEST, worker)
                    # Let the client know we dispatched the message
                    frontend.send_multipart([client, Code.ENQUEUED.value, identifier])

                # Dispatch all the results
                while results:
                    client, code, *parts = results.pop(0)
                    frontend.send_multipart([client, code.value, *parts])
                    LOGGER.info("sent %s to client %s", code, client)


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

        return apply(request, self.frontend_address, **kwargs)
