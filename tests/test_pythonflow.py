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

import os
import pickle
import random
import tempfile
import threading
import uuid

import pytest
import pythonflow as pf


def test_consistent_context():
    with pf.Graph() as graph:
        uniform = pf.func_op(random.uniform, 0, 1)
        scaled = uniform * 4
    _uniform, _scaled = graph([uniform, scaled])
    assert _scaled == 4 * _uniform


def test_context():
    with pf.Graph() as graph:
        a = pf.placeholder(name='a')
        b = pf.placeholder(name='b')
        c = pf.placeholder(name='c')
        x = a * b + c
    actual = graph(x, {a: 4, 'b': 7}, c=9)
    assert actual == 37


def test_iter():
    with pf.Graph() as graph:
        pf.constant('abc', name='alphabet', length=3)
    a, b, c = graph['alphabet']
    assert graph([a, b, c]) == tuple('abc')


def test_getattr():
    with pf.Graph() as graph:
        imag = pf.constant(1 + 4j).imag
    assert graph(imag) == 4


class MatmulDummy:
    """
    Dummy implementing matrix multiplication (https://www.python.org/dev/peps/pep-0465/) so we don't
    have to depend on numpy for the tests.
    """
    def __init__(self, value):
        self.value = value

    def __matmul__(self, other):
        if isinstance(other, pf.Operation):
            return NotImplemented
        return self.value * other


@pytest.fixture(params=[
    ('+', 1, 2),
    ('-', 3, 7.0),
    ('*', 2, 7),
    ('@', MatmulDummy(3), 4),
    ('/', 3, 2),
    ('//', 8, 3),
    ('%', 8, 5),
    ('&', 0xff, 0xe4),
    ('|', 0x01, 0xf0),
    ('^', 0xff, 0xe3),
    ('**', 2, 3),
    ('<<', 1, 3),
    ('>>', 2, 1),
    ('==', 3, 3),
    ('!=', 3, 7),
    ('>', 4, 8),
    ('>=', 9, 2),
    ('<', 7, 1),
    ('<=', 8, 7),
])
def binary_operators(request):
    operator, a, b = request.param
    expected = expected = eval('a %s b' % operator)
    return operator, a, b, expected


def test_binary_operators_left(binary_operators):
    operator, a, b, expected = binary_operators
    with pf.Graph() as graph:
        _a = pf.constant(a)
        operation = eval('_a %s b' % operator)

    actual = graph(operation)
    assert actual == expected, "expected %s %s %s == %s but got %s" % \
        (a, operator, b, expected, actual)


def test_binary_operators_right(binary_operators):
    operator, a, b, expected = binary_operators
    with pf.Graph() as graph:
        _b = pf.constant(b)
        operation = eval('a %s _b' % operator)

    actual = graph(operation)
    assert actual == expected, "expected %s %s %s == %s but got %s" % \
        (a, operator, b, expected, actual)


@pytest.mark.parametrize('operator, value', [
    ('~', True),
    ('~', False),
    ('-', 3),
    ('+', 5),
])
def test_unary_operators(value, operator):
    expected = eval('%s value' % operator)
    with pf.Graph() as graph:
        operation = eval('%s pf.constant(value)' % operator)
    actual = graph(operation)
    assert actual == expected, "expected %s %s = %s but got %s" % \
        (operator, value, expected, actual)


def test_contains():
    with pf.Graph() as graph:
        test = pf.placeholder()
        alphabet = pf.constant('abc')
        contains = pf.contains(alphabet, test)

    assert graph(contains, {test: 'a'})
    assert not graph(contains, {test: 'x'})


def test_abs():
    with pf.Graph() as graph:
        absolute = abs(pf.constant(-5))

    assert graph(absolute) == 5


def test_reversed():
    with pf.Graph() as graph:
        rev = reversed(pf.constant('abc'))

    assert list(graph(rev)) == list('cba')


def test_name_change():
    with pf.Graph() as graph:
        operation = pf.constant(None, name='operation1')
        pf.constant(None, name='operation3')

    assert 'operation1' in graph.operations
    operation.name = 'operation2'
    assert 'operation2' in graph.operations
    assert graph['operation2'] is operation
    # We cannot rename to an existing operation
    with pytest.raises(ValueError):
        operation.name = 'operation3'


@pf.opmethod(length=2)
def _split_in_two(x):
    num = len(x) // 2
    return x[:num], x[num:]


def test_parametrized_decorator():
    with pf.Graph() as graph:
        a, b = _split_in_two(pf.constant('abcd'))

    assert graph(a) == 'ab'
    assert graph(b) == 'cd'


def test_conditional():
    with pf.Graph() as graph:
        x = pf.constant(4)
        y = pf.placeholder(name='y')
        condition = pf.placeholder(name='condition')
        z = pf.conditional(condition, x, y)

    assert graph(z, condition=True) == 4
    assert graph(z, condition=False, y=5) == 5
    # We expect a value error if we evaluate the other branch without a placeholder
    with pytest.raises(ValueError):
        graph(z, condition=False)


def test_conditional_with_length():
    def f(a):
        return a, a

    with pf.Graph() as graph:
        x = pf.constant(4)
        y = pf.placeholder(name='y')
        condition = pf.placeholder(name='condition')

        z1, z2 = pf.conditional(condition, pf.func_op(f, x), pf.func_op(f, y), length=2)

    assert graph([z1, z2], condition=True) == (4, 4)
    assert graph([z1, z2], condition=False, y=5) == (5, 5)


@pytest.mark.parametrize('message', [None, "x should be smaller than %d but got %d"])
def test_assert_with_dependencies(message):
    with pf.Graph() as graph:
        x = pf.placeholder(name='x')
        if message:
            assertion = pf.assert_(x < 10, message, 10, x)
        else:
            assertion = pf.assert_(x < 10)
        with pf.control_dependencies([assertion]):
            y = 2 * x

    assert len(y.dependencies) == 1
    assert graph(y, x=9) == 18
    with pytest.raises(AssertionError) as exc_info:
        graph(y, x=11)

    if message:
        exc_info.match(message % (10, 11))


def test_assert_with_value():
    with pf.Graph() as graph:
        x = pf.placeholder(name='x')
        assertion = pf.assert_(x < 10, value=2 * x)

    assert graph(assertion, x=9) == 18
    with pytest.raises(AssertionError):
        graph(assertion, x=11)


@pytest.mark.parametrize('format_string, args, kwargs', [
    ("hello {}", ["world"], {}),
    ("hello {world}", [], {"world": "universe"}),
])
def test_str_format(format_string, args, kwargs):
    with pf.Graph() as graph:
        output = pf.str_format(format_string, *args, **kwargs)

    assert graph(output) == format_string.format(*args, **kwargs)


def test_call():
    class Adder:
        def __init__(self, a, b):
            self.a = a
            self.b = b

        def compute(self):
            return self.a + self.b

        def __call__(self):
            return self.compute()

    with pf.Graph() as graph:
        adder = pf.constant(Adder(3, 7))
        op1 = adder()
        op2 = adder.compute()

    assert graph([op1, op2]) == (10, 10)


def test_lazy_constant():
    import time

    def target():
        time.sleep(1)
        return 12345

    with pf.Graph() as graph:
        value = pf.lazy_constant(target)

    start = time.time()
    assert graph(value) == 12345
    assert time.time() - start > 1

    start = time.time()
    assert graph(value) == 12345
    assert time.time() - start < 0.01


def test_lazy_constant_not_callable():
    with pytest.raises(ValueError):
        with pf.Graph() as graph:
            pf.lazy_constant(None)


def test_graph_pickle():
    with pf.Graph() as graph:
        x = pf.placeholder('x')
        y = pf.pow_(x, 3, name='y')

    _x = random.uniform(0, 1)
    desired = graph('y', x=_x)

    pickled = pickle.dumps(graph)
    graph = pickle.loads(pickled)
    actual = graph('y', x=_x)
    assert desired == actual


def test_no_default_graph():
    with pytest.raises(ValueError):
        pf.placeholder()


def test_unpack_without_length():
    with pytest.raises(TypeError):
        with pf.Graph():
            _1, _2 = pf.placeholder()


def test_import():
    with pf.Graph() as graph:
        os_ = pf.import_('os')
        isfile = os_.path.isfile(__file__)

    assert graph(isfile)


def test_tuple():
    expected = 13
    with pf.Graph() as graph:
        a = pf.constant(expected)
        b = pf.identity((a, a))
    actual, _ = graph(b)
    assert actual is expected, "expected %s but got %s" % (expected, actual)


def test_list():
    expected = 13
    with pf.Graph() as graph:
        a = pf.constant(expected)
        b = pf.identity([a, a])
    actual, _ = graph(b)
    assert actual is expected, "expected %s but got %s" % (expected, actual)


def test_dict():
    expected = 13
    with pf.Graph() as graph:
        a = pf.constant(expected)
        b = pf.identity({'foo': a})
    actual = graph(b)['foo']
    assert actual is expected, "expected %s but got %s" % (expected, actual)


def test_slice():
    with pf.Graph() as graph:
        a = pf.constant(range(100))
        b = pf.constant(1)
        c = a[b:]

    assert len(graph(c)) == 99


def test_bool():
    with pf.Graph() as graph:
        a = pf.placeholder()

    assert a


@pytest.mark.parametrize('context, expected', [
    ({'a': 1, 'b': 0}, 'zero-division'),
    ({'a': 1, 'b': 2}, 0.5),
])
def test_try(context, expected):
    finally_reached = []

    with pf.Graph() as graph:
        a = pf.placeholder('a')
        b = pf.placeholder('b')
        c = pf.try_(
            a / b,
            [(ZeroDivisionError, 'zero-division')],
            pf.func_op(lambda: finally_reached.append('done'))
        )

    assert graph(c, context) == expected
    assert finally_reached


def test_cache():
    calls = []
    cache = {}

    @pf.opmethod
    def _cached_add(x, y):
        calls.append((x, y))
        return x + y

    with pf.Graph() as graph:
        a = pf.placeholder()
        b = pf.placeholder()
        c = pf.cache(_cached_add(a, b), cache.__getitem__, cache.setdefault)

    for _ in range(3):
        assert graph(c, {a: 1, b: 2}) == 3
        assert len(calls) == 1
        assert len(cache) == 1

    # Modify the cache value
    key, = cache.keys()
    assert key == hash((1, 2))
    cache[key] = -1
    assert graph(c, {a: 1, b: 2}) == -1

    # Check another call
    assert graph(c, {a: 1, b: 4}) == 5
    assert len(calls) == 2
    assert len(cache) == 2


def test_cache_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        with pf.Graph() as graph:
            a = pf.placeholder()
            b = pf.placeholder()
            c = pf.cache_file(a + b, f"{tmpdir}/%s.pkl")

        assert graph(c, {a: 5, b: 9}) == 14
        filename = f"{tmpdir}/{hash((5, 9))}.pkl"
        assert os.path.isfile(filename)
        # Move the file to a different location and ensure the value is loaded
        os.rename(filename, f"{tmpdir}/{hash((8, 1))}.pkl")
        assert graph(c, {a: 8, b: 1}) == 14


def test_try_not_caught():
    with pf.Graph() as graph:
        a = pf.placeholder()
        b = pf.placeholder()
        c = pf.try_(a / b, [(ValueError, 'value-error')])

    with pytest.raises(ZeroDivisionError):
        graph(c, {a: 1, b: 0})


def test_invalid_fetches():
    with pf.Graph():
        a = pf.placeholder()
    graph = pf.Graph()

    with pytest.raises(RuntimeError):
        graph(a)

    with pytest.raises(KeyError):
        graph('a')

    with pytest.raises(ValueError):
        graph(123)


def test_invalid_context():
    with pytest.raises(ValueError):
        pf.Graph().apply([], "not-a-mapping")

    with pytest.raises(ValueError):
        pf.Graph().apply([], {123: None})


def test_duplicate_value():
    with pf.Graph() as graph:
        a = pf.placeholder('a')

    with pytest.raises(ValueError):
        graph([], {a: 1}, a=1)

    with pytest.raises(ValueError):
        graph([], {a: 1, 'a': 1})


def test_conditional_callback():
    with pf.Graph() as graph:
        a = pf.constant(1)
        b = pf.constant(2)
        c = pf.placeholder()
        d = pf.conditional(c, a, b + 1)

    # Check that we have "traced" the correct number of operation evaluations
    tracer = pf.Profiler()
    assert graph(d, {c: True}, callback=tracer) == 1
    assert len(tracer.times) == 2
    tracer = pf.Profiler()
    assert graph(d, {c: False}, callback=tracer) == 3
    assert len(tracer.times) == 3


def test_try_callback():
    with pf.Graph() as graph:
        a = pf.placeholder('a')
        b = pf.assert_((a > 0).set_name('condition'), value=a, name='b')
        c = pf.try_(b, [
            (AssertionError, (pf.constant(41, name='41') + 1).set_name('alternative'))
        ])

    tracer = pf.Profiler()
    graph(c, {a: 3}, callback=tracer) == 3
    assert len(tracer.times) == 3

    graph(c, {a: -2}, callback=tracer) == 42
    assert len(tracer.times) == 5


def test_stack_trace():
    with pf.Graph() as graph:
        a = pf.placeholder()
        b = pf.placeholder()
        c = a / b

    try:
        graph(c, {a: 1, b: 0})
        raise RuntimeError("did not raise ZeroDivisionError")
    except ZeroDivisionError as ex:
        assert isinstance(ex.__cause__, pf.EvaluationError)


def test_placeholder_with_kwargs():
    with pf.Graph() as graph:
        a = pf.placeholder(length=2)
        b, c = a

    assert graph([b, c], {a: [1, 2]}) == (1, 2)


def test_thread_compatibility():
    def work(event):
        with pf.Graph() as graph:
            event.wait()
    event = threading.Event()
    thread = threading.Thread(target=work, args=(event,))
    thread.start()
    try:
        with pf.Graph() as graph:
            event.set()
    except AssertionError as e:
        event.set()
        raise e
