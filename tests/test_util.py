import pytest
import pythonflow as pf


def test_lazy_import():
    os = pf.lazy_import('os')
    _ = os.path
    missing = pf.lazy_import('some_missing_module')
    with pytest.raises(ImportError):
        _ = missing.missing_attribute


def test_batch_iterable():
    iterable = 'abcdefghijklmnopqrstuvwxyz'
    batches = pf.batch_iterable(iterable, 4)
    assert len(batches) == 7
    for i, batch in enumerate(batches):
        assert list(iterable[i * 4:(i + 1) * 4]) == batch


def test_batch_iterable_invalid_size():
    with pytest.raises(ValueError):
        pf.batch_iterable("", -1)


def test_profiling():
    with pf.Graph() as graph:
        duration = pf.placeholder('duration')
        # Use a conditional to make sure we also test the evaluation of conditionals with callbacks
        positive_duration = pf.conditional(duration < 0, 0, duration)
        sleep = pf.import_('time').sleep(positive_duration)

    callback = pf.Profiler()
    graph(sleep, callback=callback, duration=0.5)
    assert callback.times[sleep] > 0.5
    # We never evaluate duration because it is already provided in the context
    assert duration not in callback.times
    # Check that the slowest operation comes first
    callback_str = str(callback)
    assert callback_str.startswith(str(sleep))
    assert len(callback_str.split('\n')) == 4
