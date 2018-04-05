import multiprocessing
import uuid
import pytest
import pythonflow as pf
from pythonflow import pfmq


@pytest.fixture
def backend_address():
    return f'inproc://{uuid.uuid4().hex}'


@pytest.fixture
def load_balancer(backend_address):
    lb = pfmq.LoadBalancer(backend_address)
    lb.run_async()
    yield lb
    lb.cancel()


@pytest.fixture
def worker(load_balancer):
    with pf.Graph() as graph:
        x = pf.placeholder('x')
        y = pf.placeholder('y')
        z = (x + y).set_name('z')
    worker = pfmq.Worker.from_graph(graph, load_balancer.backend_address)
    worker.run_async()
    yield worker
    worker.cancel()


def test_apply(load_balancer, worker):
    request = {'fetches': 'z', 'context': {'x': 1, 'y': 3}}
    result = load_balancer.apply(request)
    assert result == 4


def test_apply_batch(load_balancer, worker):
    request = {'fetches': 'z', 'contexts': [{'x': 1, 'y': 3 + i} for i in range(5)]}
    result = load_balancer.apply(request)
    assert result == [4 + i for i in range(5)]


def test_cancel_task():
    task = pfmq.Task([], 'inproc://missing')
    task.cancel()


def test_imap(load_balancer, worker):
    requests = [{'fetches': 'z', 'context': {'x': 1, 'y': 3 + i}} for i in range(5)]
    task = load_balancer.imap(requests)
    for i, result in enumerate(task):
        assert result == i + 4
    # Make sure the task finishes
    task._thread.join()
