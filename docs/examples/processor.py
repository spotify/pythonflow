import argparse
import multiprocessing
import time

import pythonflow as pf


def create_benchmark_graph():
    with pf.Graph() as graph:
        pf.constant(None, name='fetch')
        seconds = pf.placeholder('seconds')
        pf.func_op(time.sleep, seconds, name='sleep')
    return graph


GRAPHS = {
    'benchmark': create_benchmark_graph,
}

def start_processor(args):
    graph = GRAPHS[args.graph]()
    with pf.Processor.from_graph(args.push_address, args.pull_address, graph) as processor:
        processor.run()


def entrypoint(args=None):
    """
    See

    $ python consumer.py -h

    for a detailed description.
    """
    parser = argparse.ArgumentParser('consumer', description=entrypoint.__doc__)
    parser.add_argument('--push_address', help='address to push messages to',
                        default='tcp://127.0.0.1:5556')
    parser.add_argument('--pull-address', help='address to pull messages from',
                        default='tcp://127.0.0.1:5555')
    parser.add_argument('--num-processors', '-n', help='number of fetches', type=int,
                        default=multiprocessing.cpu_count())
    parser.add_argument('graph', help='name of a graph', choices=list(GRAPHS))
    args = parser.parse_args(args)

    processors = []
    for _ in range(args.num_processors):
        processor = multiprocessing.Process(target=start_processor, args=(args,))
        processor.start()
        processors.append(processor)

    print("Started %d processors." % len(processors))

    try:
        # Wait until one of the processes terminates
        while True:
            for processor in processors:
                if processor.join(1) is not None:
                    break
    except:  # pylint: disable=broad-except
        print("Processors interrupted.")

    for processor in processors:
        processor.terminate()

    for processor in processors:
        print("Waiting for processor with pid %d..." % processor.pid)
        processor.join()

    print('Exit.')


if __name__ == '__main__':
    entrypoint()
