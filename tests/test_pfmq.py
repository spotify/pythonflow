import multiprocessing
import uuid
import pytest
import pythonflow as pf
from pythonflow import pfmq


@pytest.fixture
def backend_address():
    return f'inproc://{uuid.uuid4().hex}'


@pytest.fixture
def message_broker(backend_address):
    b = pfmq.MessageBroker(backend_address)
    b.run_async()
    yield b
    b.cancel()


@pytest.fixture
def worker(message_broker):
    with pf.Graph() as graph:
        x = pf.placeholder('x')
        y = pf.placeholder('y')
        z = (x + y).set_name('z')
    worker = pfmq.Worker.from_graph(graph, message_broker.backend_address)
    worker.run_async()
    yield worker
    worker.cancel()


def test_apply(message_broker, worker):
    request = {'fetches': 'z', 'context': {'x': 1, 'y': 3}}
    result = message_broker.apply(request)
    assert result == 4


def test_apply_batch(message_broker, worker):
    request = {'fetches': 'z', 'contexts': [{'x': 1, 'y': 3 + i} for i in range(5)]}
    result = message_broker.apply(request)
    assert result == [4 + i for i in range(5)]


def test_cancel_task():
    task = pfmq.Task([], 'inproc://missing')
    task.cancel()


def test_imap(message_broker, worker):
    requests = [{'fetches': 'z', 'context': {'x': 1, 'y': 3 + i}} for i in range(5)]
    task = message_broker.imap(requests)
    for i, result in enumerate(task):
        assert result == i + 4
    # Make sure the task finishes
    task._thread.join()
