Distributed data preparation
============================

Training data for machine learning models is often transformed before the learning process starts. But sometimes it is more convenient to generate the training data online rather than precomputing it: maybe the data we want to present to the model depends on the current state of the model, or maybe we want to experiment with different data preparation techniques without having to rerun pipelines. This document is concerned with helping you speed up the data preparation by distributing it across multiple cores or machines.

Data preprocessing can generally be expressed as :code:`sink = map(transformation, source)`, where :code:`source` is a generator of input data, :code:`transformation` is the transformation to be applied, and :code:`sink` is an iterable over the transformed data. We use `ZeroMQ <https://pyzmq.readthedocs.io/en/latest/>`_ to distribute the data processing using a `load-balancing message broker <http://zguide.zeromq.org/py:all#A-Load-Balancing-Message-Broker>`_ illustrated below. Each task (also known as a client) sends one or more messages to the broker from a :code:`source`. The broker distributes the messages amongst a set of workers that apply the desired :code:`transformation`. Finally, the broker collects the results and forwards them to the relevant task which act as a :code:`sink`.

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

    from pythonflow import pfmq

    BACKEND_ADDRESS = 'tcp://address-that-workers-should-connect-to'
    with pfmq.Worker.from_graph(graph, BACKEND_ADDRESS) as worker:
        worker.process_requests()

Using the :code:`with` statement ensures that the background thread used for communication is shut down properly and you don't end up with zombie threads. Alternatively, you can manually call the :code:`cancel` method in your teardown code. The :code:`Worker` class supports the optional keyword argument :code:`prefetch` which determines the number of messages that the worker prefetches. It defaults to :code:`1` which is appropriate in most cases. However, increasing the number of prefetched messages can be helpful when you have high latency to make sure your worker doesn't run out of messages to process.

.. note::

    Running the workers is up to you, and you can use your favourite framework such as `ipyparallel <https://ipyparallel.readthedocs.io/en/latest/>`_ or `Foreman <https://www.theforeman.org/>`_.


Consuming the data
------------------

Once you have started one or more processors, you can create a message broker to facilitate communication between tasks and workers.

.. code-block:: python

    broker = pfmq.Broker(BACKEND_ADDRESS)
    broker.run_async()

    request = {
        'fetches': 'rotated_image',
        'context': {'filename': 'docs/spotify.png'}
    }
    rotated_image = broker.apply(request)

The call to :code:`run_async` starts the message broker in a background thread. To avoid serialization overhead, only the explicitly requested :code:`fetches` are sent over the wire and the :code:`context` is not updated as in the example above.

Calling the consumer directly is useful for debugging, but in most applications you probably want to process more than one example as illustrated below.

.. code-block:: python

    iterable = broker.imap(requests)

Using :code:`imap` rather than using the built-in :code:`map` applied to :code:`broker.apply` has significant performance benefits: the message broker will dispatch as many messages as there are connected workers. In contrast, using the built-in :code:`map` will only use one worker at a given time.

.. note::

    By default, the consumer and processors will use :code:`pickle` to (de)serialize all messages. You may want to consider your own serialization format or use `msgpack <https://msgpack.org/index.html>`_.
