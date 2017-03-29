import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    pairselector = PairsStrategy1()
    constructor = PortfolioConstructor()

    def get_column_parameters(self):
        args1 = make_arg_iter(self.pairselector.get_iterable_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())
        output = {}
        for i, (x, y) in enumerate(list(itertools.product(args1, args2))):
            z = {}
            z.update(x)
            z.update(y)
            output[i] = z
        return output

    def run_index(self, index):

        data = self.read_data_from_index(index)

        args1 = make_arg_iter(self.pairselector.get_iterable_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())

        ind = 0
        output_results = pd.DataFrame()
        output_stats = {}

        for a1 in args1:
            scores, pair_info = self.pairselector.get_best_pairs(data, **a1)

            # Optimization
            self.constructor.set_and_prep_data(scores, pair_info, data)

            for a2 in args2:
                results, stats = self.constructor.get_daily_pl(**a2)
                results.columns = [ind]
                output_results = output_results.join(results, how='outer')
                output_stats[ind] = stats
                ind += 1

        self.write_index_results(output_results, index)
        self.write_index_stats(output_stats, index)

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_features(self):
        return ['AdjClose', 'AvgDolVol', 'RClose', 'RCashDividend',
                'SplitFactor', 'GSECTOR', 'EARNINGSFLAG']

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 4,
            'start_year': 2009
        }


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data', action='store_true',
        help='Run DataConstructor')
    parser.add_argument(
        '-w', '--write_simulation', action='store_true',
        help='Run simulation')
    parser.add_argument(
        '-s', '--simulation', action='store_true',
        help='Run simulation')
    args = parser.parse_args()

    if args.data:
        StatArbStrategy().make_data()
    elif args.write_simulation:
        strategy = StatArbStrategy('version_0002', True)
        strategy.start()
    elif args.simulation:
        strategy = StatArbStrategy('version_0002', False)
        strategy.start()
