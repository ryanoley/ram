import os

from ram.analysis.combo_search import CombinationSearch


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r', '--restart', action='store_true',
        help='Restart simulation')
    args = parser.parse_args()

    dpath = '/Users/mitchellsuter/Desktop/StatArbStrategy/combo_search'
    comb = CombinationSearch(dpath)

    if args.restart:
        comb.restart()

    else:
        dpath = '/Users/mitchellsuter/Desktop/StatArbStrategy/v1/'
        df = CombinationSearch._read_csv(os.path.join(dpath, 'results.csv'))
        comb.add_data(df, 'commit_1')

        dpath = '/Users/mitchellsuter/Desktop/StatArbStrategy/v2/'
        df = CombinationSearch._read_csv(os.path.join(dpath, 'results.csv'))
        comb.add_data(df, 'commit_2')

        comb.set_training_params(freq='m', n_periods=12,
                                 n_ports_per_combo=5, n_best_combos=10)
        comb.start()
