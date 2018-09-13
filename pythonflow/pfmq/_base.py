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

import enum
import uuid
import threading

import zmq


BYTEORDER = 'little'


class Code(enum.Enum):
    """
    Codes for different message types.
    """
    REQUEST = b'\x00'
    RESULT = b'\x01'
    ERROR = b'\x02'
    SERIALIZATION_ERROR = b'\x03'
    SIGN_UP = b'\x04'
    SIGN_OFF = b'\x05'
    ENQUEUED = b'\x06'
    CANCEL = b'\x07'
    TIMEOUT = b'\x08'


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

        if start:
            self.run_async()

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
            with zmq.Context.instance().socket(zmq.PAIR) as socket:  # pylint: disable=E1101
                socket.connect(self._cancel_address)
                socket.send_multipart([b''])
            self._thread.join(timeout)
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
