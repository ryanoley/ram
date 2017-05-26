import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.strategy.reversion.daily_returns import get_daily_returns


class ReversionStrategy(Strategy):

    def get_column_parameters(self):
        #args1 = make_arg_iter(self.pairselector.get_iterable_args())
        #args2 = make_arg_iter(self.constructor.get_iterable_args())
        #output_params = {}
        #for col_ind, (x, y) in enumerate(itertools.product(args1, args2)):
        #    params = dict(x)
        #    params.update(y)
        #    output_params[col_ind] = params
        #return output_params
        return []

    def run_index(self, time_index):

        data = self.read_data_from_index(time_index)

        returns = get_daily_returns(data, feature_ndays=5, holding_ndays=5,
                                    n_per_side=40)

        #args1 = make_arg_iter(self.pairselector.get_iterable_args())
        #args2 = make_arg_iter(self.constructor.get_iterable_args())
        #arg_index = 0
        #for a1 in args1:
        #    pair_info = self.pairselector.rank_pairs(data, **a1)
        #    self.constructor.set_and_prep_data(data, pair_info, time_index)
        #    for a2 in args2:
        #        results, stats = self.constructor.get_daily_pl(
        #            arg_index, **a2)
        #        self._capture_output(results, stats, arg_index)
        #        arg_index += 1
        ## Calculate returns
        #returns = self.output_pl / self.constructor.booksize
        self.write_index_results(returns, time_index)
        #self.write_index_stats(self.output_stats, time_index)

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
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 500 and AvgDolVol between 3 and 10',
            'univ_size': 500}

    def get_features(self):
        return ['AdjOpen', 'AdjClose', 'AdjVwap', 'GGROUP', 'EARNINGSFLAG'] + \
            ['RANK_PRMA10_AdjClose', 'RANK_PRMA30_AdjClose',
             'RANK_VOL30_AdjClose',
             'RANK_DISCOUNT126_AdjClose',
             'RANK_RSI10_AdjClose', 'RANK_RSI30_AdjClose',
             'RANK_MFI10_AdjClose', 'RANK_MFI30_AdjClose',
             ]

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 2,
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
        '-p', '--prepped_data', default='version_0004',
        help='Run simulation')
    args = parser.parse_args()

    if args.data:
        ReversionStrategy().make_data()
    elif args.write_simulation:
        print('Running for data: {}'.format(args.prepped_data))
        strategy = ReversionStrategy(args.prepped_data, True)
        strategy.start()
    elif args.simulation:
        strategy = ReversionStrategy(args.prepped_data, False)
        import pdb; pdb.set_trace()
        strategy.start()
