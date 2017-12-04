import argparse
import itertools as it
import multiprocessing
import re
import textwrap
import time

import numpy as np
import pythonflow as pf
import tqdm


def parse_tuple(value):
    """
    Parse a context tuple of the format 'key=type(value)'.
    """
    match = re.match(r'(\w+)=(\w+)\((.*?)\)', value)
    assert match, "could not parse '%s'" % value
    return match.group(1), eval(match.group(2))(match.group(3))


def entrypoint(args=None):
    """
    The consumer process provides a simple command line interface to test the performance of one
    or more processors.

    To test the performance of the ZMQ communication, start one or more benchmark processors from
    the command line like so:

    $ python processor.py -n <number of processors> benchmark

    Omitting the `-n` flag will run as many processors as your machine has CPUs.

    Then run the consumer like so:

    $ python consumer.py benchmark

    Which will print summary statistics about how long it took to process the data.

    To test whether the distributed processing speeds up your data preparation, we provide a simple
    example. Start the consumer like so:

    $ python consumer.py -c "seconds=float(0.1)" sleep

    Each processor will sleep for 100 ms, and, for a single processor, you should see just under ten
    iterations per second. Adding additional processors should lead to an (almost) linear increase
    in the number of iterations per second.
    """
    parser = argparse.ArgumentParser('consumer', description=entrypoint.__doc__)
    parser.add_argument('--push_address', help='address to push messages to',
                        default='tcp://127.0.0.1:5555')
    parser.add_argument('--pull-address', help='address to pull messages from',
                        default='tcp://127.0.0.1:5556')
    parser.add_argument('--num-fetches', '-n', help='number of fetches', type=int,
                        default=100)
    parser.add_argument('--max-messages', '-m', help='maximum number of messages to publish',
                        type=int, default=3 * multiprocessing.cpu_count())
    parser.add_argument('--context', '-c', help='context information', nargs='+', default=[],
                        type=parse_tuple)
    parser.add_argument('fetches', help='names of operations to fetch', nargs='+')
    args = parser.parse_args(args)

    context = dict(args.context)
    print("Context: %s" % context)

    with pf.Consumer(args.push_address, args.pull_address) as consumer:
        contexts = [context for _ in range(args.num_fetches)]
        iterator = consumer.map(args.fetches, contexts, max_messages=args.max_messages)

        times = []
        for _ in tqdm.tqdm(iterator, total=args.num_fetches):
            times.append(time.time())

    deltas = np.diff(times) * 1000
    summary = textwrap.dedent(
        """
        Summary statistics for %d fetches in ms
        =======================================
        Minimum         : %.3f
        5th percentile  : %.3f
        25th percentile : %.3f
        Median          : %.3f
        75th percentile : %.3f
        95th percentile : %.3f
        Maximum         : %.3f
        ---------------------------------------
        Mean            : %.3f
        Standard dev.   : %.3f
        ---------------------------------------

        Iterations per second: %.1f
        """ % (
            args.num_fetches, np.min(deltas), np.percentile(deltas, 5),
            np.percentile(deltas, 25), np.median(deltas), np.percentile(deltas, 75),
            np.percentile(deltas, 95), np.max(deltas), np.mean(deltas), np.std(deltas),
            1000 / np.mean(deltas)
        ))
    print(summary)

if __name__ == '__main__':
    entrypoint()
