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

import collections
import contextlib
import functools
import importlib
import operator
import uuid


class Graph:
    """
    Data flow graph constituting a directed acyclic graph of operations.
    """
    def __init__(self):
        self.operations = {}
        self.dependencies = []

    default_graph = None

    def __enter__(self):
        assert self.default_graph is None, "cannot have more than one default graph"
        Graph.default_graph = self
        return self

    def __exit__(self, *args):
        assert self.default_graph is self
        Graph.default_graph = None

    def normalize_operation(self, operation):  # pylint:disable=W0621
        """
        Normalize an operation by resolving its name if necessary.

        Parameters
        ----------
        operation : Operation or str
            Operation instance or name of an operation.

        Returns
        -------
        normalized_operation : Operation
            Operation instance.

        Raises
        ------
        ValueError
            If `operation` is not an `Operation` instance or an operation name.
        RuntimeError
            If `operation` is an `Operation` instance but does not belong to this graph.
        KeyError
            If `operation` is an operation name that does not match any operation of this graph.
        """
        if isinstance(operation, Operation):
            if operation.graph is not self:
                raise RuntimeError("operation '%s' does not belong to this graph" % operation)
            return operation
        elif isinstance(operation, str):
            return self.operations[operation]
        else:
            raise ValueError("'%s' is not an `Operation` instance or operation name" % operation)

    def normalize_context(self, context, **kwargs):
        """
        Normalize a context by replacing all operation names with operation instances.

        Parameters
        ----------
        context : dict[Operation or str, object]
            Context whose keys are operation instances or names.
        kwargs : dict[str, object]
            Additional context information keyed by variable name.

        Returns
        -------
        normalized_context : dict[Operation, object]
            Normalized context whose keys are operation instances.

        Raises
        ------
        ValueError
            If the context specifies more than one value for any operation.
        ValueError
            If `context` is not a mapping.
        """
        if context is None:
            context = {}
        elif not isinstance(context, collections.Mapping):
            raise ValueError("`context` must be a mapping.")

        operations = list(context)
        for operation in operations:  # pylint:disable=W0621
            value = context.pop(operation)
            operation = self.normalize_operation(operation)
            if operation in context:
                raise ValueError("duplicate value for operation '%s'" % operation)
            context[operation] = value

        # Add the keyword arguments
        for name, value in kwargs.items():
            operation = self.operations[name]
            if operation in context:
                raise ValueError("duplicate value for operation '%s'" % operation)
            context[operation] = value

        return context

    def __call__(self, fetches, context=None, *, callback=None, **kwargs):
        """
        Evaluate one or more operations given a context.

        Parameters
        ----------
        fetches : list[str or Operation] or str or Operation
            One or more `Operation` instances or names to evaluate.
        context : dict or None
            Context in which to evaluate the operations.
        callback : callable or None
            Callback to be evaluated when an operation is evaluated.
        kwargs : dict
            Additional context information keyed by variable name.

        Returns
        -------
        values : tuple[object]
            Output of the operations given the context.

        Raises
        ------
        ValueError
            If `fetches` is not an `Operation` instance, operation name, or a sequence thereof.
        ValueError
            If `context` is not a mapping.
        """
        if isinstance(fetches, (str, Operation)):
            fetches = [fetches]
            single = True
        elif isinstance(fetches, collections.Sequence):
            single = False
        else:
            raise ValueError("`fetches` must be an `Operation` instance, operation name, or a "
                             "sequence thereof.")

        fetches = [self.normalize_operation(operation) for operation in fetches]
        context = self.normalize_context(context, **kwargs)
        values = [fetch.evaluate(context, callback=callback) for fetch in fetches]
        return values[0] if single else tuple(values)

    def __getitem__(self, name):
        return self.operations[name]

    @staticmethod
    def get_active_graph(graph=None):
        """
        Obtain the currently active graph instance by returning the explicitly given graph or using
        the default graph.

        Parameters
        ----------
        graph : Graph or None
            Graph to return or `None` to use the default graph.

        Raises
        ------
        ValueError
            If no `Graph` instance can be obtained.
        """
        graph = graph or Graph.default_graph
        if not graph:
            raise ValueError("`graph` must be given explicitly or a default graph must be set")
        return graph


class Operation:  # pylint:disable=too-few-public-methods
    """
    Base class for operations.

    Parameters
    ----------
    args : tuple
        Positional arguments passed to the `_evaluate` method.
    kwargs : dict
        Keyword arguments passed to the `_evaluate` method.
    length : int or None
        Optional number of values returned by the operation. The length only needs to be specified
        if the operation should support iterable
        [unpacking](https://www.python.org/dev/peps/pep-3132/).
    graph : Graph or None
        Data flow graph for this operation or `None` to use the default graph.
    name : str or None
        Name of the operation or `None` to use a random, unique identifier.
    dependencies : list
        Explicit sequence of operations to evaluate before evaluating this operation.
    """
    def __init__(self, *args, length=None, graph=None, name=None, dependencies=None, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.length = length
        self.graph = Graph.get_active_graph(graph)
        # Choose a name for the operation and add the operation to the graph
        self._name = None
        self.name = name or uuid.uuid4().hex
        # Get a list of all dependencies relevant to this operation
        self.dependencies = [] if dependencies is None else dependencies
        self.dependencies.extend(self.graph.dependencies)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, data):
        self.__dict__.update(data)

    @property
    def name(self):
        """str : Unique name of the operation"""
        return self._name

    @name.setter
    def name(self, name):
        self.set_name(name)

    def set_name(self, name):
        """
        Set the name of the operation and update the graph.

        Parameters
        ----------
        value : str
            Unique name of the operation.

        Returns
        -------
        self : Operation
            This operation.

        Raises
        ------
        ValueError
            If an operation with `value` already exists in the associated graph.
        KeyError
            If the current name of the operation cannot be found in the associated graph.
        """
        if name in self.graph.operations:
            raise ValueError("duplicate name '%s'" % name)
        if self._name is not None:
            self.graph.operations.pop(self._name)
        self.graph.operations[name] = self
        self._name = name
        return self

    def evaluate_dependencies(self, context, callback=None):
        """
        Evaluate the dependencies of this operation and discard the values.

        Parameters
        ----------
        context : dict
            Normalised context in which to evaluate the operation.
        callback : callable or None
            Callback to be evaluated when an operation is evaluated.
        """
        for operation in self.dependencies:
            operation.evaluate(context, callback)

    def evaluate(self, context, callback=None):
        """
        Evaluate the operation given a context.

        Parameters
        ----------
        context : dict
            Normalised context in which to evaluate the operation.
        callback : callable or None
            Callback to be evaluated when an operation is evaluated.

        Returns
        -------
        value : object
            Output of the operation given the context.
        """
        # Evaluate all explicit dependencies first
        self.evaluate_dependencies(context, callback)

        if self in context:
            return context[self]
        # Evaluate the parents
        partial = functools.partial(self.evaluate_operation, context=context, callback=callback)
        args = [partial(arg) for arg in self.args]
        kwargs = {key: partial(value) for key, value in self.kwargs.items()}
        # Evaluate the operation
        if callback:
            with callback(self, context):
                context[self] = value = self._evaluate(*args, **kwargs)
        else:
            context[self] = value = self._evaluate(*args, **kwargs)

        return value

    def _evaluate(self, *args, **kwargs):
        """
        Inheriting operations should implement this function to evaluate the operation.
        """
        raise NotImplementedError

    @staticmethod
    def evaluate_operation(operation, context, **kwargs):
        """
        Evaluate an operation or constant given a context.
        """
        return operation.evaluate(context, **kwargs) if isinstance(operation, Operation) \
            else operation

    def __hash__(self):
        return id(self)

    def __len__(self):
        if self.length is None:
            raise TypeError('`length` must be specified explicitly for operations')
        return self.length

    def __iter__(self):
        num = len(self)
        for i in range(num):
            yield self[i]

    # pylint: disable=E1123
    def __getattr__(self, name):
        return getattr_(self, name, graph=self.graph)

    def __getitem__(self, key):
        return getitem(self, key, graph=self.graph)

    def __add__(self, other):
        return add(self, other, graph=self.graph)

    def __radd__(self, other):
        return add(other, self, graph=self.graph)

    def __sub__(self, other):
        return sub(self, other, graph=self.graph)

    def __rsub__(self, other):
        return sub(other, self, graph=self.graph)

    def __pow__(self, other):
        return pow_(self, other, graph=self.graph)

    def __rpow__(self, other):
        return pow_(other, self, graph=self.graph)

    def __mul__(self, other):
        return mul(self, other, graph=self.graph)

    def __matmul__(self, other):
        return matmul(self, other, graph=self.graph)

    def __rmatmul__(self, other):
        return matmul(other, self, graph=self.graph)

    def __rmul__(self, other):
        return mul(other, self, graph=self.graph)

    def __truediv__(self, other):
        return truediv(self, other, graph=self.graph)

    def __rtruediv__(self, other):
        return truediv(other, self, graph=self.graph)

    def __floordiv__(self, other):
        return floordiv(self, other, graph=self.graph)

    def __rfloordiv__(self, other):
        return floordiv(other, self, graph=self.graph)

    def __mod__(self, other):
        return mod(self, other, graph=self.graph)

    def __rmod__(self, other):
        return mod(other, self, graph=self.graph)

    def __lshift__(self, other):
        return lshift(self, other, graph=self.graph)

    def __rlshift__(self, other):
        return lshift(other, self, graph=self.graph)

    def __rshift__(self, other):
        return rshift(self, other, graph=self.graph)

    def __rrshift__(self, other):
        return rshift(other, self, graph=self.graph)

    def __and__(self, other):
        return and_(self, other, graph=self.graph)

    def __rand__(self, other):
        return and_(other, self, graph=self.graph)

    def __or__(self, other):
        return or_(self, other, graph=self.graph)

    def __ror__(self, other):
        return or_(other, self, graph=self.graph)

    def __xor__(self, other):
        return xor(self, other, graph=self.graph)

    def __rxor__(self, other):
        return xor(other, self, graph=self.graph)

    def __lt__(self, other):
        return lt(self, other, graph=self.graph)

    def __le__(self, other):
        return le(self, other, graph=self.graph)

    def __eq__(self, other):
        return eq(self, other, graph=self.graph)

    def __ne__(self, other):
        return ne(self, other, graph=self.graph)

    def __gt__(self, other):
        return gt(self, other, graph=self.graph)

    def __ge__(self, other):
        return ge(self, other, graph=self.graph)

    def __invert__(self):
        return inv(self, graph=self.graph)

    def __neg__(self):
        return neg(self, graph=self.graph)

    def __abs__(self):
        return abs_(self, graph=self.graph)

    def __pos__(self):
        return pos(self, graph=self.graph)

    def __reversed__(self):
        return reversed_(self, graph=self.graph)

    def __call__(self, *args, **kwargs):
        return call(self, *args, **kwargs)
    # pylint: enable=E1123


class func_op(Operation):  # pylint: disable=C0103,R0903
    """
    Operation wrapper for stateless functions.

    Parameters
    ----------
    target : callable
        function to evaluate the operation
    args : tuple
        positional arguments passed to the target
    kwargs : dict
        keywoard arguments passed to the target
    """
    def __init__(self, target, *args, **kwargs):
        super(func_op, self).__init__(*args, **kwargs)
        self.target = target

    def _evaluate(self, *args, **kwargs):
        return self.target(*args, **kwargs)

    def __repr__(self):
        return "<pf.func_op '%s' target=%s args=<%d items> kwargs=<%d items>>" % \
            (self.name, self.target, len(self.args), len(self.kwargs))


def opmethod(target=None, **kwargs):
    """
    Decorator for creating operations from functions.
    """
    # This is called when the decorator is used with arguments
    if target is None:
        return functools.partial(opmethod, **kwargs)

    # This is called when the decorator is used without arguments
    @functools.wraps(target)
    def _wrapper(*args, **kwargs_inner):
        return func_op(target, *args, **kwargs_inner, **kwargs)
    return _wrapper


# pylint: disable=C0103
abs_ = opmethod(abs)
add = opmethod(operator.add)
and_ = opmethod(operator.and_)
contains = opmethod(operator.contains)
eq = opmethod(operator.eq)
floordiv = opmethod(operator.floordiv)
ge = opmethod(operator.ge)
getattr_ = opmethod(getattr)
getitem = opmethod(operator.getitem)
gt = opmethod(operator.gt)
inv = opmethod(operator.inv)
le = opmethod(operator.le)
lshift = opmethod(operator.lshift)
lt = opmethod(operator.lt)
matmul = opmethod(operator.matmul)
methodcaller = opmethod(operator.methodcaller)
mod = opmethod(operator.mod)
mul = opmethod(operator.mul)
ne = opmethod(operator.ne)
neg = opmethod(operator.neg)
not_ = opmethod(operator.not_)
or_ = opmethod(operator.or_)
pos = opmethod(operator.pos)
pow_ = opmethod(operator.pow)
rshift = opmethod(operator.rshift)
sub = opmethod(operator.sub)
truediv = opmethod(operator.truediv)
xor = opmethod(operator.xor)
map_ = opmethod(map)
sum_ = opmethod(sum)
zip_ = opmethod(zip)
filter_ = opmethod(filter)
list_ = opmethod(list)
tuple_ = opmethod(tuple)
reversed_ = opmethod(reversed)
print_ = opmethod(print)
format_ = opmethod(format)
import_ = opmethod(importlib.import_module)
# pylint: enable=C0103


@opmethod
def call(func, *args, **kwargs):
    """
    Call `func` with positional arguments `args` and keyword arguments `kwargs`.

    Parameters
    ----------
    func : callable
        Function to call when the operation is executed.
    args : list
        Sequence of positional arguments passed to `func`.
    kwargs : dict
        Mapping of keyword arguments passed to `func`.
    """
    return func(*args, **kwargs)


@contextlib.contextmanager
def control_dependencies(dependencies, graph=None):
    """
    Ensure that all `dependencies` are executed before any operations in this scope.

    Parameters
    ----------
    dependencies : list
        Sequence of operations to be evaluted before evaluating any operations defined in this
        scope.
    """
    # Add dependencies to the graph
    graph = Graph.get_active_graph(graph)
    graph.dependencies.extend(dependencies)
    yield
    # Remove dependencies from the graph
    del graph.dependencies[-len(dependencies):]
