import random
import threading
import time
import uuid

import pytest

import pythonflow as pf
from pythonflow import pfmq


def join_thread(thread, timeout=None):
    thread.join(timeout)
    if thread.isAlive():
        raise RuntimeError("thread %s is still alive" % thread)


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
        sleep = pf.func_op(time.sleep, pf.func_op(random.uniform, 0, .1))
        with pf.control_dependencies([sleep]):
            (x / y).set_name('z')
        # Can't pickle entire modules
        pf.constant(pf).set_name('not_pickleable')

    # Create multiple workers
    _workers = []
    _threads = []
    while len(_workers) < 10:
        worker = pfmq.Worker.from_graph(graph, broker.backend_address)
        worker.run_async()
        thread = threading.Thread(target=worker.process_requests)
        thread.start()
        _threads.append(thread)
        _workers.append(worker)

    yield _workers

    # Shut down all the workers
    for worker in _workers:
        worker.cancel()

    # Ensure all the threads have closed down
    for thread in _threads:
        join_thread(thread, 1)


@pytest.fixture
def requests():
    return [{'fetches': 'z', 'context': {'x': 1, 'y': 3 + i}} for i in range(200)]


def test_workers_running(workers):
    # Sleep a second to make sure the workers have "time to fail"
    time.sleep(1)
    for worker in workers:
        assert worker.is_alive


def test_apply(broker, workers):
    request = {'fetches': 'z', 'context': {'x': 1, 'y': 3}}
    result = broker.apply(request)
    assert result == 1 / 3


def test_apply_error(broker, workers):
    request = {'fetches': 'z', 'context': {'x': 1, 'y': 0}}
    with pytest.raises(ZeroDivisionError):
        broker.apply(request)


def test_apply_batch(broker, workers):
    request = {'fetches': 'z', 'contexts': [{'x': 1, 'y': 3 + i} for i in range(5)]}
    result = broker.apply(request)
    assert result == [1 / (3 + i) for i in range(5)]


def test_cancel_task():
    task = pfmq.Task([], 'inproc://missing')
    task.cancel()
    join_thread(task._thread, 1)


def test_imap(broker, workers, requests):
    task = broker.imap(requests)
    for i, result in enumerate(task):
        assert result == 1 / (3 + i)
    # Make sure the task finishes
    join_thread(task._thread, 1)


def test_imap_unordered(broker, workers, requests):
    task = broker.imap(requests, ordered=False)
    results = list(task)
    # Make sure the task finishes
    join_thread(task._thread, 1)

    # Ensure the ordered results are ok
    for i, result in enumerate(sorted(results, reverse=True)):
        assert result == 1 / (3 + i)

    # With high probability at least one result will be out of order
    assert any(a != b for a, b in zip(results, sorted(results)))


def test_task_context(broker, workers, requests):
    with broker.imap(requests, cache_size=1) as task:
        pass
    # Make sure the task finishes
    join_thread(task._thread, 1)


def test_task_context_not_started(broker, workers, requests):
    with broker.imap(requests, start=False) as task:
        assert task.is_alive
    join_thread(task._thread, 1)


def test_worker_timeout(backend_address):
    worker = pfmq.Worker(lambda: None, backend_address, timeout=.1, num_retries=3, start=False)
    start = time.time()
    worker.run()
    duration = time.time() - start
    assert duration > .3


def test_task_timeout(backend_address):
    task = pfmq.Task([0, 1, 3], backend_address, timeout=.1, num_retries=3, start=False)
    start = time.time()
    task.run()
    duration = time.time() - start
    assert duration > .3
    with pytest.raises(TimeoutError):
        list(task)


def test_cancel_not_running(broker):
    broker.cancel()
    assert not broker.is_alive
    broker.cancel()


def test_imap_not_running(broker):
    broker.cancel()
    with pytest.raises(RuntimeError):
        broker.imap([])


def test_apply_not_running(broker):
    broker.cancel()
    with pytest.raises(RuntimeError):
        broker.apply(None)


def test_not_pickleable(broker, workers):
    with pytest.raises(pfmq.SerializationError):
        broker.apply({'fetches': 'not_pickleable', 'context': {}})


def test_no_context(broker, workers):
    with pytest.raises(KeyError):
        broker.apply({})


def test_worker_not_running():
    worker = pfmq.Worker(None, None, start=False)
    with pytest.raises(RuntimeError):
        worker.process_requests()
