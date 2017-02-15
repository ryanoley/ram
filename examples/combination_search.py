import os

from ram.analysis.combo_search import CombinationSearch


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r', '--restart', action='store_true',
        help='Restart simulation')
    parser.add_argument(
        '-n', '--search_name',
        help='Name of directory')
    args = parser.parse_args()

    BASE = 'C:/Users/Mitchell/Desktop/StatArbStrategy'
    dpath = os.path.join(BASE, args.search_name)
    comb = CombinationSearch(dpath)

    if args.restart:
        comb.restart()

    else:
        dpath = os.path.join(BASE, 'v1')
        df = CombinationSearch._read_csv(os.path.join(dpath, 'results.csv'))
        comb.add_data(df, 'commit_1')

        dpath = os.path.join(BASE, 'v2')
        df = CombinationSearch._read_csv(os.path.join(dpath, 'results.csv'))
        comb.add_data(df, 'commit_2')

        dpath = os.path.join(BASE, 'v3')
        df = CombinationSearch._read_csv(os.path.join(dpath, 'results.csv'))
        comb.add_data(df, 'commit_3')

        dpath = os.path.join(BASE, 'v4')
        df = CombinationSearch._read_csv(os.path.join(dpath, 'results.csv'))
        comb.add_data(df, 'commit_4')

        comb.set_training_params(freq='w', n_periods=104,
                                 n_ports_per_combo=5, n_best_combos=10)
        comb.start()
