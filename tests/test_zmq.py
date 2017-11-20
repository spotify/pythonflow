import multiprocessing
import pytest
import pythonflow as pf


def run_processor():
    with pf.Graph() as graph:
        x = pf.placeholder('x')
        a = pf.placeholder('a')
        y = (a * x).set_name('y')

    with pf.Processor.from_graph('tcp://127.0.0.1:5555', 'tcp://127.0.0.1:5556', graph) as processor:
        processor.run()


@pytest.fixture
def processor_process():
    process = multiprocessing.Process(target=run_processor)
    process.start()
    yield process
    process.terminate()
    process.join()


@pytest.fixture
def consumer(processor_process):
    with pf.Consumer('tcp://127.0.0.1:5556', 'tcp://127.0.0.1:5555') as consumer:
        yield consumer


def test_call(consumer):
    assert consumer('y', {'x': 2}, a=3) == 6


def test_map(consumer):
    actual = consumer.map('y', [{'x': x} for x in range(5)], a=3)
    expected = (0, 3, 6, 9, 12)
    for a, b in zip(actual, expected):
        assert a == b


def test_map_batch(consumer):
    actual = consumer.map('y', [{'x': x} for x in range(5)], a=3, batch_size=3)
    expected = [(0, 3, 6), (9, 12)]
    for a, b in zip(actual, expected):
        assert a == b
