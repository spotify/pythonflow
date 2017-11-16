# pylint: disable=missing-docstring
# pylint: enable=missing-docstring
# Copyright 2017 Spotify AB
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

import pickle
import queue
import threading
import uuid
import zmq


class ZeroBase:  # pylint: disable=too-few-public-methods
    """
    Base class for ZeroMQ communication.

    Parameters
    ----------
    push_address : str
        Address to which messages are pushed.
    pull_address : str
        Address from which messages are pulled.
    dumps : callable
        Function to serialize messages.
    loads : callable
        Function to deserialize messages.
    """
    IDENTIFIER_SIZE = 16

    def __init__(self, push_address, pull_address, method, dumps=None, loads=None):  # pylint: disable=too-many-arguments
        self.push_address = push_address
        self.pull_address = pull_address

        self.context = zmq.Context()

        self.pusher = self.context.socket(zmq.PUSH)  # pylint: disable=no-member
        try:
            getattr(self.pusher, method)(self.push_address)
        except zmq.ZMQError as ex:  # pragma: no cover
            raise RuntimeError("failed to %s pusher to '%s': %s" % (method, self.push_address, ex))

        self.puller = self.context.socket(zmq.PULL)  # pylint: disable=no-member
        try:
            getattr(self.puller, method)(self.pull_address)
        except zmq.ZMQError as ex:  # pragma: no cover
            raise RuntimeError("failed to %s puller to '%s': %s" % (method, self.pull_address, ex))

        self.dumps = dumps or pickle.dumps
        self.loads = loads or pickle.loads

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        """
        Close the associated sockets.
        """
        self.pusher.close()
        self.puller.close()


class Consumer(ZeroBase):
    """
    Data consumer.

    Parameters
    ----------
    push_address : str
        Address to which messages are pushed.
    pull_address : str
        Address from which messages are pulled.
    max_messages : str
        Maximum number of messages that are published at any time.
    dumps : callable
        Function to serialize messages (defaults to `pickle.dumps`).
    loads : callable
        Function to deserialize messages (defaults to `pickle.loads`).
    """
    def __init__(self, push_address, pull_address, max_messages, dumps=None, loads=None):  # pylint: disable=too-many-arguments
        super(Consumer, self).__init__(push_address, pull_address, 'bind', dumps, loads)
        if max_messages <= 0:
            raise ValueError('`max_messages` must be positive but got %s' % max_messages)
        self.max_messages = max_messages

    def push_message(self, message, identifier_queue=None):
        """
        Push a message.

        Parameters
        ----------
        message : object
            Message to be pushed.
        identifier_queue : queue.Queue
            Queue used to keep track of the order of messages.

        Returns
        -------
        identifier : bytes
            Unique identifier of the message.
        """
        identifier = uuid.uuid4().bytes
        if identifier_queue:
            identifier_queue.put(identifier, timeout=1)
        self.pusher.send(identifier + self.dumps(message))
        return identifier

    def push_messages(self, messages, stop_event, identifier_queue):
        """
        Push a sequence of messages.

        Parameters
        ----------
        messages : iterable
            Sequence of messages to push.
        stop_event : threading.Event
            Event used to stop pushing messages.
        identifier_queue : queue.Queue
            Queue used to keep track of the order of messages.
        """
        messages = iter(messages)
        message = next(messages)
        while not stop_event.is_set():
            try:
                self.push_message(message, identifier_queue)
                message = next(messages)
            except queue.Full:  # pragma: no cover
                pass
            except StopIteration:
                identifier_queue.put(None)
                break

    def wait_for_message(self, identifier, cache):
        """
        Wait for a specific message.

        Parameters
        ----------
        identifier : bytes
            Unique identifier of the message to wait for.
        cache : dict
            Cache for storing messages.

        Returns
        -------
        payload : object
            Response payload.
        """
        while identifier not in cache:
            message = self.puller.recv()
            _identifier = message[:self.IDENTIFIER_SIZE]
            status = message[self.IDENTIFIER_SIZE]
            payload = self.loads(message[1 + self.IDENTIFIER_SIZE:])
            if status:
                raise payload
            else:
                cache[_identifier] = payload

        return cache.pop(identifier)

    def get_message(self, message):
        """
        Push a message and wait for the response.

        Parameters
        ----------
        message : object
            Message to be pushed.

        Returns
        -------
        payload : object
            Response payload.
        """
        identifier = self.push_message(message)
        return self.wait_for_message(identifier, {})

    @staticmethod
    def build_message(fetches, context, **kwargs):
        """
        Build a message to be processed by a remote graph.
        """
        for key in context:
            if not isinstance(key, str):
                raise TypeError("context keys must be strings but got '%s'" % key)
        # Add the keyword arguments
        for key, value in kwargs.items():
            if key in context:
                raise ValueError("duplicate value for key '%s'" % key)
            context[key] = value
        return {
            'fetches': fetches,
            'context': context,
        }

    def map_messages(self, messages):
        """
        Push a sequence of messages and wait for the responses.

        Parameters
        ----------
        messages : iterable
            Sequence of messages to push.

        Yields
        ------
        payload : object
            Response payload.
        """
        try:
            # Publish all the messages in a background thread
            identifier_queue = queue.Queue(self.max_messages)
            stop_event = threading.Event()
            thread = threading.Thread(
                target=self.push_messages,
                args=(messages, stop_event, identifier_queue)
            )
            thread.start()

            cache = {}

            while True:
                # Get the next identifier
                identifier = identifier_queue.get()
                if identifier is None:
                    return

                yield self.wait_for_message(identifier, cache)
        except:  # pragma: no cover
            stop_event.set()
            raise
        finally:
            # Wait for the publishing thread to exit
            thread.join()

    def __call__(self, fetches, context, **kwargs):
        """
        Evaluate one or more operations remotely given a context.

        Parameters
        ----------
        fetches : list[str or Operation] or str or Operation
            One or more `Operation` instances or names to evaluate.
        context : dict or None
            Context in which to evaluate the operations.
        kwargs : dict
            Additional context information keyed by variable name.

        Returns
        -------
        values : tuple[object]
            Output of the operations given the context.
        """
        return self.get_message(self.build_message(fetches, context, **kwargs))

    def map(self, fetches, contexts, **kwargs):
        """
        Evaluate one or more operations remotely given a sequence of contexts.

        Parameters
        ----------
        fetches : list[str or Operation] or str or Operation
            One or more `Operation` instances or names to evaluate.
        contexts : list[dict or None]
            Sequence of contexts in which to evaluate the operations.
        kwargs : dict
            Additional context information keyed by variable name that is shared across all
            contexts.
        """
        messages = map(lambda context: self.build_message(fetches, context, **kwargs), contexts)
        return self.map_messages(messages)


class Producer(ZeroBase):  # pragma: no cover
    """
    Data producer.

    Parameters
    ----------
    push_address : str
        Address to which messages are pushed.
    pull_address : str
        Address from which messages are pulled.
    target : callable
        Function to process incoming messages.
    dumps : callable
        Function to serialize messages.
    loads : callable
        Function to deserialize messages.
    """
    def __init__(self, push_address, pull_address, target, dumps=None, loads=None):  # pylint: disable=too-many-arguments
        super(Producer, self).__init__(push_address, pull_address, 'connect', dumps, loads)
        self.target = target

    def run(self):
        """
        Run the data producer event loop.
        """
        while True:
            message = self.puller.recv()
            identifier = message[:self.IDENTIFIER_SIZE]
            status = b'\x00'
            try:
                payload = self.loads(message[self.IDENTIFIER_SIZE:])
                try:
                    payload = self.target(payload)
                except Exception as payload:  # pylint: disable=broad-except
                    status = '\x02'
            except Exception as payload:  # pylint: disable=broad-except
                status = '\x01'
            self.pusher.send(b''.join([identifier, status, self.dumps(payload)]))

    @classmethod
    def from_graph(cls, push_address, pull_address, graph, dumps=None, loads=None):  # pylint: disable=too-many-arguments
        """
        Create a producer from a graph.

        Deserialized messages must be dictionaries with `fetches` and `context` keys.

        Parameters
        ----------
        push_address : str
            Address to which messages are pushed.
        pull_address : str
            Address from which messages are pulled.
        graph : Graph
            Graph to process messages.
        dumps : callable
            Function to serialize messages.
        loads : callable
            Function to deserialize messages.
        """
        return cls(push_address, pull_address,
                   lambda payload: graph(payload['fetches'], payload['context']),
                   dumps, loads)
