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

import io
import logging
import random
import uuid
import pythonflow as pf
import pytest


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
    assert graph([a, b, c]) == list('abc')


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


@pytest.mark.parametrize('level', ['debug', 'info', 'warning', 'error', 'critical'])
def test_logger(level):
    with pf.Graph() as graph:
        logger = pf.Logger(uuid.uuid4().hex)
        log1 = logger.log(level, "this is a %s message", "test")
        log2 = getattr(logger, level)("this is another %s message", "test")

    # Add a handler to the logger
    stream = io.StringIO()
    logger.logger.setLevel(logging.DEBUG)
    logger.logger.addHandler(logging.StreamHandler(stream))
    graph([log1, log2])
    assert stream.getvalue() == "this is a test message\nthis is another test message\n"


@pytest.mark.parametrize('format_string, args, kwargs', [
    ("hello {}", ["world"], {}),
    ("hello {world}", [], {"world": "universe"}),
])
def test_str_format(format_string, args, kwargs):
    with pf.Graph() as graph:
        output = pf.str_format(format_string, *args, **kwargs)

    assert graph(output) == format_string.format(*args, **kwargs)
