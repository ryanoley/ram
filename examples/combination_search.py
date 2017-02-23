import os

from ram.analysis.combo_search import CombinationSearch


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r', '--restart', action='store_true',
        help='Restart simulation')
    args = parser.parse_args()

    comb = CombinationSearch('StatArbStrategy')

    if args.restart:
        comb.restart()
    else:
        comb.start('run_0001')
