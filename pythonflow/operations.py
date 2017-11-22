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

import functools
import logging

from .core import opmethod, Operation, func_op


class placeholder(Operation):  # pylint: disable=C0103,R0903
    """
    Placeholder that needs to be given in the context to be evaluated.
    """
    def __init__(self, name=None):
        super(placeholder, self).__init__(name=name)

    def _evaluate(self):  # pylint: disable=W0221
        raise ValueError("missing value for placeholder '%s'" % self.name)

    def __repr__(self):
        return "<pf.placeholder '%s'>" % self.name


class conditional(Operation):  # pylint: disable=C0103,W0223
    """
    Return `x` if `predicate` is `True` and `y` otherwise.

    Note that the conditional operation will only execute one branch of the computation graph
    depending on `predicate`.
    """
    def __init__(self, predicate, x, y=None, *, name=None, dependencies=None):  # pylint: disable=W0235
        super(conditional, self).__init__(predicate, x, y, name=name, dependencies=dependencies)

    def evaluate(self, context, callback=None):
        # Evaluate all dependencies first
        self.evaluate_dependencies(context)

        predicate, x, y = self.args  # pylint: disable=E0632,C0103
        # Evaluate the predicate and pick the right operation
        predicate = self.evaluate_operation(predicate, context)
        if callback:
            with callback(self, context):
                context[self] = value = self.evaluate_operation(x if predicate else y, context)
        else:
            context[self] = value = self.evaluate_operation(x if predicate else y, context)
        return value

@opmethod
def identity(value):
    """
    Operation returning the input value.
    """
    return value


# Short hand for the identity
constant = identity  # pylint: disable=invalid-name


@opmethod
def assert_(condition, message=None, *args, value=None):
    """
    Return `value` if the `condition` is satisfied and raise an `AssertionError` with the specified
    `message` and `args` if not.
    """
    if message:
        assert condition, message % args
    else:
        assert condition

    return value


@opmethod
def str_format(format_string, *args, **kwargs):
    """
    Use python's advanced string formatting to convert the format string and arguments.

    References
    ----------
    https://www.python.org/dev/peps/pep-3101/
    """
    return format_string.format(*args, **kwargs)


class Logger:
    """
    Wrapper for a standard python logging channel with the specified `logger_name`.

    Parameters
    ----------
    logger_name : str
        Name of the underlying standard python logger.

    Attributes
    ----------
    logger : logging.Logger
        Underlying standard python logger.
    """
    def __init__(self, logger_name=None):
        self.logger = logging.getLogger(logger_name)

    @functools.wraps(logging.Logger.log)
    def log(self, level, message, *args, **kwargs):  # pylint: disable=missing-docstring
        if isinstance(level, str):
            level = getattr(logging, level.upper())
        return func_op(self.logger.log, level, message, *args, **kwargs)

    @functools.wraps(logging.Logger.debug)
    def debug(self, message, *args, **kwargs):  # pylint: disable=missing-docstring
        return func_op(self.logger.debug, message, *args, **kwargs)

    @functools.wraps(logging.Logger.info)
    def info(self, message, *args, **kwargs):  # pylint: disable=missing-docstring
        return func_op(self.logger.info, message, *args, **kwargs)

    @functools.wraps(logging.Logger.warning)
    def warning(self, message, *args, **kwargs):  # pylint: disable=missing-docstring
        return func_op(self.logger.warning, message, *args, **kwargs)

    @functools.wraps(logging.Logger.error)
    def error(self, message, *args, **kwargs):  # pylint: disable=missing-docstring
        return func_op(self.logger.error, message, *args, **kwargs)

    @functools.wraps(logging.Logger.critical)
    def critical(self, message, *args, **kwargs):  # pylint: disable=missing-docstring
        return func_op(self.logger.critical, message, *args, **kwargs)


class lazy_constant(Operation):  # pylint: disable=invalid-name
    """
    Operation that returns the output of `target` lazily.

    Parameters
    ----------
    target : callable
        Function to evaluate when the operation is evaluated.
    kwargs : dict
        Keyword arguments passed to the constructor of `Operation`.
    """
    def __init__(self, target, **kwargs):
        super(lazy_constant, self).__init__(**kwargs)
        self.target = target
        if not callable(self.target):
            raise ValueError("`target` must be callable")
        self.value = None

    def _evaluate(self):  # pylint: disable=W0221
        if self.value is None:
            self.value = self.target()
        return self.value
