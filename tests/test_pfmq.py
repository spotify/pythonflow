import multiprocessing
import time
import uuid
import pytest
import pythonflow as pf
from pythonflow import pfmq


@pytest.fixture
def backend_address():
    return f'inproc://{uuid.uuid4().hex}'


@pytest.fixture
def broker(backend_address):
    b = pfmq.Broker(backend_address)
    b.run_async()
    yield b
    b.cancel()


@pytest.fixture
def workers(broker):
    with pf.Graph() as graph:
        x = pf.placeholder('x')
        y = pf.placeholder('y')
        z = (x + y).set_name('z')

    # Create multiple workers
    _workers = []
    while len(_workers) < 3:
        worker = pfmq.Worker.from_graph(graph, broker.backend_address)
        worker.run_async()
        _workers.append(worker)

    yield _workers

    # Shut down all the workers
    for worker in _workers:
        worker.cancel()


def test_workers_running(workers):
    # Sleep a second to make sure the workers have "time to fail"
    time.sleep(1)
    for worker in workers:
        assert worker.is_alive


def test_apply(broker, workers):
    request = {'fetches': 'z', 'context': {'x': 1, 'y': 3}}
    result = broker.apply(request)
    assert result == 4


def test_apply_batch(broker, workers):
    request = {'fetches': 'z', 'contexts': [{'x': 1, 'y': 3 + i} for i in range(5)]}
    result = broker.apply(request)
    assert result == [4 + i for i in range(5)]


def test_cancel_task():
    task = pfmq.Task([], 'inproc://missing')
    task.cancel()
    task._thread.join()


def test_imap(broker, workers):
    requests = [{'fetches': 'z', 'context': {'x': 1, 'y': 3 + i}} for i in range(20)]
    task = broker.imap(requests)
    for i, result in enumerate(task):
        assert result == i + 4
    # Make sure the task finishes
    task._thread.join()


def test_worker_timeout(backend_address):
    worker = pfmq.Worker(lambda: None, backend_address, timeout=.1, max_retries=3)
    start = time.time()
    worker.run()
    duration = time.time() - start
    assert duration > .3


def test_task_timeout(backend_address):
    task = pfmq.Task([0, 1, 3], backend_address, timeout=.1, max_retries=3)
    start = time.time()
    task.run()
    duration = time.time() - start
    assert duration > .3
