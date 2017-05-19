import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.statarb.pairselector.pairs3 import PairSelector3
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor
from ram.strategy.statarb.constructor.constructor3 import PortfolioConstructor3


class StatArbStrategy(Strategy):

    # Creates on init
    pairselector = PairSelector3()
    constructor = PortfolioConstructor3()

    def get_column_parameters(self):
        args1 = make_arg_iter(self.pairselector.get_iterable_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())
        output_params = {}
        for col_ind, (x, y) in enumerate(itertools.product(args1, args2)):
            params = dict(x)
            params.update(y)
            output_params[col_ind] = params
        return output_params

    def run_index(self, time_index):
        data = self.read_data_from_index(time_index)
        args1 = make_arg_iter(self.pairselector.get_iterable_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())
        arg_index = 0
        for a1 in args1:
            pair_info = self.pairselector.rank_pairs(data, **a1)
            self.constructor.set_and_prep_data(data, pair_info, time_index)
            for a2 in args2:
                results, stats = self.constructor.get_daily_pl(
                    arg_index, **a2)
                self._capture_output(results, stats, arg_index)
                arg_index += 1
        # Calculate returns
        returns = self.output_pl / self.constructor.booksize
        self.write_index_results(returns, time_index)
        self.write_index_stats(self.output_stats, time_index)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, stats, arg_index):
        if arg_index == 0:
            self.output_pl = pd.DataFrame()
            self.output_exposure = pd.DataFrame()
            self.output_stats = {}
        results.columns = [arg_index, arg_index]
        self.output_pl = self.output_pl.join(results.iloc[:, 0], how='outer')
        self.output_exposure = self.output_exposure.join(
            results.iloc[:, 1], how='outer')
        self.output_stats[arg_index] = stats

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR in (25) and AvgDolVol >= 10 ' +
            'and Close_ between 15 and 1000',
            'univ_size': 200}

    def get_features(self):
        return ['AdjClose', 'AdjVolume', 'AvgDolVol',
                'RClose', 'RCashDividend',
                'SplitFactor', 'GSECTOR', 'GGROUP', 'EARNINGSFLAG']

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 4,
            'test_period_length': 2,
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
    parser.add_argument(
        '-p', '--prepped_data', default='version_0017',
        help='Run simulation')
    args = parser.parse_args()

    if args.data:
        StatArbStrategy().make_data()
    elif args.write_simulation:
        print('Running for data: {}'.format(args.prepped_data))
        strategy = StatArbStrategy(args.prepped_data, True)
        strategy.start()
    elif args.simulation:
        strategy = StatArbStrategy(args.prepped_data, False)
        import pdb; pdb.set_trace()
        strategy.start()
