Using Pythonflow
================

Pythonflow is a pure python library for `dataflow programming <https://en.wikipedia.org/wiki/Dataflow_programming>`_. In contrast to the usual `control flow <https://en.wikipedia.org/wiki/Control_flow>`_ paradigm of python, dataflow programming requires two steps. First, we set up a `directed acyclic graph <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_ (DAG) of operations that represents the computation we want to perform. Second, we evaluate the operations of the DAG by providing input values for some of the nodes of the graph. But learning by example is usually easier than theory. So here we go.

.. testcode::

   import pythonflow as pf


   with pf.Graph() as graph:
       a = pf.constant(4)
       b = pf.constant(38)
       x = a + b

The code above creates a simple graph which adds two numbers. Importantly, none of the computation has been performed yet, and `x` is an operation that must be evaluated to obtain the result of the addition:

.. doctest::

   >>> x
   <pf.func_op '...' target=<built-in function add> args=<2 items> kwargs=<0 items>>

Although a little cryptic, the output tells us that :code:`x` is an operation that wraps the built-in function :code:`add`, has two positional arguments, and no keyword arguments. We can evaluate the operation to perform the computation by calling the :code:`graph` like so.

.. doctest::

   >>> graph(x)
   42

pythonflow supports all operations of the standard python `data model <https://docs.python.org/3/reference/datamodel.html>`_ that do not change the state of the operation (think `:code:`const` member functions <https://isocpp.org/wiki/faq/const-correctness#const-member-fns>`_) in C++) because operations are assumed to be stateless. For example, :code:`b = a + 1` is allowed whereas :code:`a += 1` is not. However, we do not believe this imposes significant restrictions.

.. _some_background:

Some background
---------------

Computations are encoded as a DAG where operations are represented by nodes and dependencies between operations are represented by edges. Operations are usually stateless, i.e. they provide the same output given the same input (see :ref:`new_operations_3` for an exception). All state information is provided by the :code:`context`, which is a mapping from nodes to values.

When Pythonflow evaluates an operation, it will check whether the current :code:`context` provides a value for the operation and return it immediately if possible. If the current :code:`context` does not provide a value for the operation, Pythonflow will evaluate the dependencies of the operation, evaluate the operation of interest, store the computed value in the context, and return the value.

Referring to operations
-----------------------

Operations can be referred to using their python instances or their unique :code:`name` attribute. By default, :code:`name` is set to a random but unique identifier. The name of an operation can be specified when they are created, e.g.

.. testcode::

   with pf.Graph() as graph:
       a = pf.constant(4)
       b = pf.constant(38)
       x = pf.add(a, b, name='my_addition')

.. doctest::

   >>> x.name
   'my_addition'

Or the name of an operation can be changed after it has been created (as long as the name is unique within each graph), e.g.

.. testcode::

   with pf.Graph() as graph:
       a = pf.constant(4)
       b = pf.constant(38)
       x = a + b
       x.name = 'my_addition'

Finally, you can change an operation's name using its :code:`set_name` method, e.g.

.. testcode::

   with pf.Graph() as graph:
       a = pf.constant(4)
       b = pf.constant(38)
       x = (a + b).set_name('my_addition')

Once a name has been set, an operation can be evaluated like so

.. doctest::

   >>> graph('my_addition')
   42

Pythonflow will enforce that names are indeed unique.

.. testcode::

   with pf.Graph() as graph:
       a = pf.constant(4, name='constant1')
       b = pf.constant(38, name='constant1')

.. testoutput::

   Traceback (most recent call last):
   ValueError: duplicate name 'constant1'


Providing inputs
----------------

Inputs for the dataflow graph can be provided using placeholders.

.. testcode::

   with pf.Graph() as graph:
       a = pf.placeholder(name='first_input')
       b = pf.constant(4)
       x = a + b

.. doctest::

   >>> graph(x, {a: 5})
   9

.. doctest::

   >>> graph(x, {'first_input': 8})
   12

.. doctest::

   >>> graph(x, first_input=7)
   11

The latter two options are only available if the operation has been given a sensible name. Pythonflow will make sure that you do not provide inconsistent inputs:

.. doctest::

   >>> graph(x, {a: 5}, first_input=7)
   Traceback (most recent call last):
   ValueError: duplicate value for operation '<pf.placeholder 'first_input'>'

And that all necessary placeholders have been specified:

.. doctest::

   >>> graph(x)
   Traceback (most recent call last):
   ValueError: missing value for placeholder 'first_input'


Handling sequences
------------------

Unfortunately, Pythonflow does not support list comprehensions but the same results can be achieved using :code:`pf.map_`, :code:`pf.list_`, :code:`pf.tuple_`, :code:`pf.zip_`, :code:`pf.sum_`, :code:`pf.filter_` and other operations. Suppose we want to find the surnames of all artists whose first name begins with an :code:`A`.


.. testcode::

   with pf.Graph() as graph:
       artists = pf.placeholder(name='artists')
       filtered = pf.filter_(lambda artist: artist['first'].startswith('A'), artists)
       surnames = pf.map_(lambda artist: artist['last'], filtered)
       # Convert to a list to evaluate the map call
       surnames = pf.list_(surnames)

.. doctest::

   >>> graph(surnames, artists=[
   ...     {
   ...         'first': 'Ariana',
   ...         'last': 'Grande'
   ...     },
   ...     {
   ...         'first': 'Justin',
   ...         'last': 'Bieber'
   ...     }
   ... ])
   ['Grande']

Adding your own operations
--------------------------

Sometimes the operations that come out of the box aren't enough for your needs and you want to build something more sophisticated. There are three different options for adding new operations and we will cover each in turn.

Turning functions into operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the :code:`pf.func_op` class to create an operation from a `callable`. The syntax is identical to :code:`partial` `functions <https://docs.python.org/3/library/functools.html#functools.partial>`_ except that the arguments are operations rather than values.

.. testcode::

   import random
   random.seed(1)

   with pf.Graph() as graph:
       uniform = pf.func_op(random.uniform, 0, 1)
       scaled_uniform = 10 * uniform

.. doctest::

   >>> graph([uniform, scaled_uniform])
   (0.13436424411240122, 1.3436424411240122)

The example above not only shows how to use existing functions as operations but also illustrates that each operation is evaluated at most once when you call :code:`graph`. Consequently, any computationally expensive operations are automatically cached.

Writing new operations using decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you only ever intend to use a callable as an operation, you can implement the operation using a decorator:

.. testcode::

   @pf.opmethod(length=2)
   def split_in_two(x):
       num = len(x)
       return x[:num // 2], x[num // 2:]


   with pf.Graph() as graph:
       x = pf.placeholder()
       y, z = split_in_two(x)

.. doctest::

   >>> graph([y, z], {x: 'Hello World!'})
   ('Hello ', 'World!')

You may use the :code:`opmethod` decorator with or without parameters. Specifying the :code:`length` parameter enables unpacking of operations as illustrated above. However, this means that your operations must not have a parameter called `length` that you set using a keyword argument (positional arguments are fine). If you are wrapping an existing method that takes a :code:`length` argument in a :code:`func_op`, use a :code:`lambda` function to rename the parameter like so.

.. testcode::

   def existing_function_you_cannot_change(length):
       return 'a' * length

   with pf.Graph() as graph:
       length = pf.placeholder()
       # Rename the keyword argument using a lambda function
       y = pf.func_op(lambda length_: existing_function_you_cannot_change(length_), length_=length)
       # Positional arguments don't cause any trouble
       z = pf.func_op(existing_function_you_cannot_change, length)

.. doctest::

   >>> graph([y, z], {length: 3})
   ('aaa', 'aaa')


.. _new_operations_3:


Writing new operations using :code:`pf.Operation`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to create stateful operations, you need to dig a bit deeper into pythonflow. For example, stateful operations may be useful when you need to access a database but don't want to open a new connection every time you send a request. Stateful operations are implemented by inheriting from `pf.Operation` and implementing the :code:`_evaluate` method like so.

.. testcode::

   import sqlite3


   class SqliteOperation(pf.Operation):
       def __init__(self, database, query):
           # Pass on the query as an operation
           super(SqliteOperation, self).__init__(query)
           # Open a new database connection
           self.database = database
           self.connection = sqlite3.connect(self.database)

       def _evaluate(self, query):
           # The `_evaluate` method takes the same arguments as the `__init__` method
           # of the super class. Whereas the `__init__` method of the superclass receives
           # operations as arguments, the `__call__` method receives the evaluated
           # operations
           return self.connection.execute(query)


   with pf.Graph() as graph:
       query = pf.placeholder(name='query')
       response = SqliteOperation(':memory:', query)

.. doctest::

   >>> graph(response, query='CREATE TABLE Companies (name VARCHAR)')
   <sqlite3.Cursor object at ...>
   >>> graph(response, query="INSERT INTO Companies (name) VALUES ('Spotify')")
   <sqlite3.Cursor object at ...>
   >>> graph(response, query="SELECT * FROM Companies").fetchall()
   [('Spotify',)]

Conditional operations
----------------------

Sometimes you may want to evaluate different parts of the DAG depending on a condition. For example, you may want to apply the same operations to data but switch between training and validation data like so.


.. testcode::

   with pf.Graph() as graph:
       training_data = pf.placeholder("training")
       validation_data = pf.placeholder("validation")
       condition = pf.placeholder("condition")
       data = pf.conditional(condition, training_data, validation_data)

.. doctest::

   >>> graph(data, condition=True, training=4)
   4

Note that the :code:`pf.conditional` operation only evaluates the part of the DAG it requires to return a value. If it evaluated the entire graph, the evaluation above would have raised a :code:`ValueError` because we did not provide a value for the placeholder :code:`validation_data`.

Explicitly controlling dependencies
-----------------------------------

Pythonflow automatically determines the operations it needs to evaluate to return the desired output. But sometimes it is desirable to explicitly specify operations that should be evaluated. For example, you may want to print a value for debugging purposes like so.

.. testcode::

   with pf.Graph() as graph:
       x = pf.placeholder('x')
       y = pf.mul(2, x, dependencies=[pf.print_(pf.str_format("placeholder value: {}", x))])

.. doctest::

   >>> graph(y, x=4)
   placeholder value: 4
   8

You may also use the context manager :code:`control_dependencies` to specifiy explicit dependencies like so.


.. testcode::

   with pf.Graph() as graph:
       x = pf.placeholder('x')
       with pf.control_dependencies([pf.print_(pf.str_format("placeholder value: {}", x))]):
           y = 2 * x

.. doctest::

   >>> graph(y, x=9)
   placeholder value: 9
   18

Assertions
----------

When you're developing your graphs, you probably want to make sure that everything is behaving as you expect. You can check that values conform to your expectations like so.


.. testcode::

   with pf.Graph() as graph:
       mass = pf.placeholder('mass')
       height = pf.placeholder('height')
       assertions = [
           pf.assert_(mass > 0, "mass must be positive but got %f", mass),
           pf.assert_(height > 0, "height must be positive but got %f", height)
       ]
       with pf.control_dependencies(assertions):
           bmi = mass / height ** 2

.. doctest::

   >>> graph(bmi, mass=72, height=-1.8)
   Traceback (most recent call last):
   AssertionError: height must be positive but got -1.800000

To make the definition of graphs less verbose, you can also specify the return value of an assertion should it succeed using the `value` keyword argument like so.

.. testcode::

   with pf.Graph() as graph:
       mass = pf.placeholder('mass')
       height = pf.placeholder('height')
       mass = pf.assert_(mass > 0, "mass must be positive but got %f", mass, value=mass)
       height = pf.assert_(height > 0, "height must be positive but got %f", height, value=height)
       bmi = mass / height ** 2

.. doctest::

   >>> graph(bmi, mass=72, height=-1.8)
   Traceback (most recent call last):
   AssertionError: height must be positive but got -1.800000


Logging
-------

No software is complete without the ability to log information for later analysis or monitoring. Pythonflow supports logging through the standard python `logging module <https://docs.python.org/3/library/logging.html>`_ like so.


.. testcode::

   import logging
   import sys
   logging.basicConfig(stream=sys.stdout)

   with pf.Graph() as graph:
       logger = pf.Logger()
       tea_temperature = pf.placeholder('tea_temperature')
       with pf.control_dependencies([pf.conditional(tea_temperature > 80, logger.warning('the tea is too hot'))]):
           tea_temperature = pf.identity(tea_temperature)

.. doctest::

   >>> graph(tea_temperature, tea_temperature=85)  # doctest: +SKIP
   WARNING:root:the tea is too hot
   85

Profiling and callbacks
-----------------------

If your graph doesn't perform as well as you would like, you can gain some insight into where it's spending its time by attaching a profiler. For example, the following graph loads an image and applies a few transformations.

.. literalinclude:: examples/image_transformation.py

.. testsetup:: profiling

    exec(open('docs/examples/image_transformation.py').read(), globals(), locals())

.. testcode:: profiling

    profiler = pf.Profiler()
    graph('rotated_image', filename='docs/spotify.png', callback=profiler)

Printing the profiler shows the ten most expensive operations. More detailed information can be retrieved by accessing the :code:`times` attribute or calling :code:`get_slow_operations`.

.. doctest:: profiling

    >>> print(profiler)  # doctest: +SKIP
    <pf.func_op 'rotated_image' target=<function call at 0x7fa6fff3abf8> args=<3 items> kwargs=<1 items>>: 0.014486551284790039
    <pf.func_op 'imread' target=<function call at 0x7fa6fff3abf8> args=<2 items> kwargs=<0 items>>: 0.003396749496459961
    <pf.func_op '214e8b1e1db94dfa9840d9cc4e510c25' target=<function call at 0x7fa6fff3abf8> args=<4 items> kwargs=<0 items>>: 0.0013699531555175781
    <pf.func_op 'image' target=<built-in function truediv> args=<2 items> kwargs=<0 items>>: 0.0005080699920654297
    <pf.func_op 'noisy_image' target=<built-in function mul> args=<2 items> kwargs=<0 items>>: 0.00014448165893554688
    <pf.func_op 'noise' target=<built-in function sub> args=<2 items> kwargs=<0 items>>: 0.000125885009765625
    <pf.func_op '2acc029ccdb34007a826a3af3230a483' target=<function import_module at 0x7fa71a86f488> args=<1 items> kwargs=<0 items>>: 3.838539123535156e-05
    <pf.func_op '5b342c50ea0c48d0b136cb6d93fdc579' target=<function import_module at 0x7fa71a86f488> args=<1 items> kwargs=<0 items>>: 2.8133392333984375e-05
    <pf.func_op '583f053d792245c4bad181e2430eb8d2' target=<built-in function getitem> args=<2 items> kwargs=<0 items>>: 2.0265579223632812e-05
    <pf.func_op '9f39f2b5e0d74fe695099cd78d8ecd11' target=<function import_module at 0x7fa71a86f488> args=<1 items> kwargs=<0 items>>: 1.9788742065429688e-05

Rotating and reading the image from disk are the two most expensive operations.

The profiler is implemented as a callback that is passed to the graph when fetches are evaluated. Callbacks are `context managers <https://docs.python.org/3/reference/datamodel.html#with-statement-context-managers>`_ whose context is entered before an operation is evaluated and exited after the operation has been evaluated. Callbacks must accept exactly two arguments: the operation under consideration and the context. Here is an example that prints some information about the evaluation of the fetches.

.. testcode:: profiling

    import contextlib


    @contextlib.contextmanager
    def print_summary(operation, context):
        print("About to evaluate '%s', %d values in the context." % (operation.name, len(context)))
        yield
        print("Evaluated '%s', %d values in the context." % (operation.name, len(context)))

.. doctest:: profiling

    >>> graph('rotated_image', filename='docs/spotify.png', callback=print_summary)  # doctest: +SKIP
    About to evaluate '2acc029ccdb34007a826a3af3230a483', 1 values in the context.
    Evaluated '2acc029ccdb34007a826a3af3230a483', 2 values in the context.
    About to evaluate '60f266f8dd69441eb25dea412a92dbb0', 2 values in the context.
    Evaluated '60f266f8dd69441eb25dea412a92dbb0', 3 values in the context.
    About to evaluate '9f39f2b5e0d74fe695099cd78d8ecd11', 3 values in the context.
    Evaluated '9f39f2b5e0d74fe695099cd78d8ecd11', 4 values in the context.
    About to evaluate '7e6579c9d3784500a1dd555e3ea81bde', 4 values in the context.
    Evaluated '7e6579c9d3784500a1dd555e3ea81bde', 5 values in the context.
    About to evaluate 'imread', 5 values in the context.
    Evaluated 'imread', 6 values in the context.
    About to evaluate '583f053d792245c4bad181e2430eb8d2', 6 values in the context.
    Evaluated '583f053d792245c4bad181e2430eb8d2', 7 values in the context.
    About to evaluate 'image', 7 values in the context.
    Evaluated 'image', 8 values in the context.
    About to evaluate '5b342c50ea0c48d0b136cb6d93fdc579', 8 values in the context.
    Evaluated '5b342c50ea0c48d0b136cb6d93fdc579', 9 values in the context.
    About to evaluate '748ad66b9338417594015a71486b324f', 9 values in the context.
    Evaluated '748ad66b9338417594015a71486b324f', 10 values in the context.
    About to evaluate '240dcda966e34cfb957d4b91e4e31a4f', 10 values in the context.
    Evaluated '240dcda966e34cfb957d4b91e4e31a4f', 11 values in the context.
    About to evaluate 'noise_scale', 11 values in the context.
    Evaluated 'noise_scale', 12 values in the context.
    About to evaluate 'c44468bd3aac44ddb8b3e8bd2b9eb832', 12 values in the context.
    Evaluated 'c44468bd3aac44ddb8b3e8bd2b9eb832', 13 values in the context.
    About to evaluate '214e8b1e1db94dfa9840d9cc4e510c25', 13 values in the context.
    Evaluated '214e8b1e1db94dfa9840d9cc4e510c25', 14 values in the context.
    About to evaluate 'noise', 14 values in the context.
    Evaluated 'noise', 15 values in the context.
    About to evaluate 'noisy_image', 15 values in the context.
    Evaluated 'noisy_image', 16 values in the context.
    About to evaluate '08bd356d6d464837a6a367c34591c961', 16 values in the context.
    Evaluated '08bd356d6d464837a6a367c34591c961', 17 values in the context.
    About to evaluate '8fa2493c78174b2f9d9849afa4e7e1f5', 17 values in the context.
    Evaluated '8fa2493c78174b2f9d9849afa4e7e1f5', 18 values in the context.
    About to evaluate '6c1d0576cadc418b8f037455fec5c749', 18 values in the context.
    Evaluated '6c1d0576cadc418b8f037455fec5c749', 19 values in the context.
    About to evaluate 'rotated_image', 19 values in the context.
    Evaluated 'rotated_image', 20 values in the context.
