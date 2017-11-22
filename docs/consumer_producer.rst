Distributed data preparation
============================

Training data for machine learning models is often transformed before the learning process starts. But sometimes it is more convenient to generate the training data online rather than precomputing it: Maybe the data we want to present to the model depends on the current state of the model, or maybe we want to experiment with different data preparation techniques without having to rerun pipelines. This document is concerned with helping you speed up the data preparation by distributing it across multiple cores or machines.

.. Pythonflow provides the ability to experiment with data processing pipelines by allowing you to evaluate any part of the computational graph and modify the values of any node--for example to perform `ceiling analysis <https://www.coursera.org/learn/machine-learning/lecture/LrJbq/ceiling-analysis-what-part-of-the-pipeline-to-work-on-next>`_.

Data preprocessing can generally be expressed as :code:`sink = map(processor, source)`, where :code:`source` is a generator of input data, :code:`processor` is the transformation to be applied, and :code:`sink` is an iterable over the transformed data. We use `ZeroMQ <https://pyzmq.readthedocs.io/en/latest/>`_ to distribute the data processing using a `pipeline pattern <http://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html>`_: one or more :code:`processors` can transform data on the same or different machines to speed up your data preparation.

Building an image transformation graph
--------------------------------------

For example, you may want to speed up the process of applying a transformation to an image using the following graph.

.. literalinclude:: examples/image_transformation.py

Let's run the default pipeline using the Spotify logo.

.. code-block:: python

    context = {'filename': 'docs/spotify.png'}
    graph('rotated_image', context)

Because the context keeps track of all computation steps, we have access to the original image, the noisy image, and the rotated image:

.. plot::

    exec(open('examples/image_transformation.py').read(), globals(), locals())

    context = {'filename': 'spotify.png'}
    graph('rotated_image', context)

    from matplotlib import pyplot as plt
    import numpy as np

    fig, axes = plt.subplots(1, 3, True, True)
    axes[0].imshow(context[image])
    axes[0].set_title('Original')
    axes[1].imshow(context[noisy_image])
    axes[1].set_title('Noisy')
    axes[2].imshow(np.maximum(context[rotated_image], 0))
    axes[2].set_title('Rotated')
    plt.show()

Distributing the workload
-------------------------

Pythonflow provides a straightforward interface to turn your graph into a processor for distributed data preparation:

.. code-block:: python

    push_address = 'tcp://address-to-push-transformed-data-to'
    pull_address = 'tcp://address-to-pull-input-data-from'

    with pf.Processor.from_graph(push_address, pull_address, graph) as processor:
        processor.run()

Running the processors is up to you and you can use your favourite framework such as `ipyparallel <https://ipyparallel.readthedocs.io/en/latest/>`_ or `Foreman <https://www.theforeman.org/>`_.


Consuming the data
------------------

Once you have started one or more processors, you can create a :code:`Consumer` to transform data remotely like so.

.. code-block:: python

    push_address = 'tcp://address-to-push-input-data-to'
    pull_address = 'tcp://address-to-pull-transformed-data-from'

    consumer = pf.Consumer(push_address, pull_address)
    rotated_image = consumer('rotated_image', filename='docs/spotify.png')
    consumer.close()

To avoid serialization overhead, only the explicitly requested :code:`fetches` are sent over the wire and the :code:`context` is not updated as in the example above.

Calling the consumer directly is useful for debugging. You can apply the processors across a sequence of input arguments like so:

.. code-block:: python

    iterable = consumer.map('rotated_image', some_sequence_of_contexts)

By default, the consumer will only publish one message at any point in time because it does not know how many processors there are. You can achieve better performance by setting the keyword argument :code:`max_messages` to a small integer multiple of the number of processors you have. Setting :code:`max_messages` too low will mean that the processors spend most of their time idling and waiting for messages to process. Setting it too high will mean that you exhaust your computer memory if the sequence of contexts is large.

You can also batch the data by setting the :code:`batch_size` argument to an integer larger than one such that :code:`iterable` will yield batches of the desired size.

.. note::

    By default, the consumer and processors will use :code:`pickle` to (de)serialize all messages. You may want to consider your own serialization format or use `msgpack <https://msgpack.org/index.html>`_.
