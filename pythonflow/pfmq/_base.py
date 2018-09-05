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
import os
import threading
import uuid

import zmq

LOGGER = logging.getLogger(__name__)


class TopicError(RuntimeError):
    """
    Topic mismatch.
    """

class Base:
    """
    Base class for running a ZeroMQ event loop in a background thread with a PAIR channel for
    cancelling the background thread.

    Parameters
    ----------
    start : bool
        Whether to start the event loop as a background thread.
    topic : bytes or None
        Topic for the communication. Only sockets with the same topic are allowed to communicate
        with one another to avoid unexpected communication with unintended remote graphs.
    """
    def __init__(self, start, topic=None):
        self._thread = None
        self._cancel_address = f'inproc://{uuid.uuid4().hex}'
        self._cancel_parent = zmq.Context.instance().socket(zmq.PAIR)  # pylint: disable=E1101
        self._cancel_parent.bind(self._cancel_address)

        if isinstance(topic, str):
            topic = topic.encode()
        self.topic = topic or os.environb.get(b'PFMQ_TOPIC', b'default')

        if start:
            self.run_async()

    STATUS = {
        'end': b'\x01',
        'response_error': b'\x02',
        'timeout': b'\x03',
        'serialization_error': b'\x04',
        'topic_error': b'\x05',
        'dispatch': b'\x06',
        'poll': b'\x07',
        'request': b'\x08',
        'response': b'\x09',
        'sign_up': b'\x10',
    }
    STATUS.update({value: key for key, value in STATUS.items()})

    def __enter__(self):
        self.run_async()
        return self

    def __exit__(self, *_):
        self.cancel()

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

        Returns
        -------
        cancelled : bool
            Whether the background thread was cancelled. `False` if the background thread was not
            running.
        """
        if self.is_alive:
            self._cancel_parent.send_multipart([b''])
            self._thread.join(timeout)
            self._cancel_parent.close()
            return True
        return False

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
