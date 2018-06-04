.. pythonflow documentation master file, created by
   sphinx-quickstart on Wed Sep  6 16:03:02 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pythonflow: Dataflow programming for python.
============================================

Pythonflow is a simple implementation of `dataflow programming <https://en.wikipedia.org/wiki/Dataflow_programming>`_ for python. Users of `Tensorflow <https://www.tensorflow.org/>`_ will immediately be familiar with the syntax.

At Spotify, we use Pythonflow in data preprocessing pipelines for machine learning models because

* it automatically caches computationally expensive operations,
* any part of the computational graph can be easily evaluated for debugging purposes,
* it allows us to distribute data preprocessing across multiple machines.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   install
   guide
   distributed
   pythonflow


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
