Installation of Pythonflow
==========================

This section guides you through the installation of Pythonflow, which only supports python 3.

Installation for users
----------------------

You can install Pythonflow from the command line like so.

.. code-block:: bash

    $ pip install pythonflow

If you don't have :code:`pip` installed, have a look at `this guide <http://docs.python-guide.org/en/latest/starting/installation/>`_.

Installation for developers
---------------------------

If you want to contribute to Pythonflow, we recommend you fork the `GitHub repository of Pythonflow <https://github.com/spotify/pythonflow>`_ and clone your fork like so.

.. code-block:: bash

    $ git clone git@github.com:<your username>/pythonflow.git

Next, create a `virtual environment <http://docs.python-guide.org/en/latest/dev/virtualenvs/>`_ or set up a `Conda environment <https://conda.io/miniconda.html>`_ and install Pythonflow in development mode like so.

.. code-block:: bash

    $ pip install -e /path/to/your/clone/of/pythonflow

Testing your installation
~~~~~~~~~~~~~~~~~~~~~~~~~

To make sure your local installation of Pythonflow works as expected (and to make sure your changes don't break a test when you submit a pull request), you can run Pythonflow's unit tests. First, install the requirements needed for tests like so.

.. code-block:: bash

    $ pip install -r requirements/test.txt

Then run the tests like so.

.. code-block:: bash

    $ pylint pythonflow
    $ py.test --cov --cov-fail-under=100 --cov-report=term-missing

We have also provided a :code:`Makefile` so you don't have to remember the commands for testing and you can run the following instead.

.. code-block:: bash

    $ make tests

For more details on how to contribute, have a look at `GitHub's developer guide <https://guides.github.com/introduction/flow/>`_ and make sure you have read our `guidelines for contributing <https://github.com/spotify/pythonflow/blob/master/CONTRIBUTING.md>`_.

Building the documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to see what the documentation of your local changes looks like, first install the requirements needed for compiling it like so.

.. code-block:: bash

    $ pip install -r dev-requirements.txt

Then compile the documentation like so.

.. code-block:: bash

    $ make docs
