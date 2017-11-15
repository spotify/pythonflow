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
