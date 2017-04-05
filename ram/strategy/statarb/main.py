import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.statarb.pairselector.pairs import PairSelector
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    # Creates on init
    pairselector = PairSelector()
    constructor = PortfolioConstructor()

    def get_column_parameters(self):
        args1 = make_arg_iter(self.pairselector.get_iterable_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())
        output_params = {}
        for col_ind, (x, y) in enumerate(itertools.product(args1, args2)):
            params = dict(x)
            params.update(y)
            output_params[col_ind] = params
        return output_params

    def run_index(self, index):

        data = self.read_data_from_index(index)

        args1 = make_arg_iter(self.pairselector.get_iterable_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())

        pair_index = 0
        port_index = 0
        for a1 in args1:

            pair_info = self.pairselector.rank_pairs(data, **a1)

            self.constructor.set_and_prep_data(data, pair_info)
            pair_index += 1
            for a2 in args2:
                results, stats = self.constructor.get_daily_pl(
                    port_index, **a2)
                self._capture_output(results, stats)
                port_index += 1
        self.write_index_results(self.output_results, index)
        self.write_index_stats(self.output_stats, index)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, stats):
        if not hasattr(self, 'output_results'):
            ind = 0
            self.output_results = pd.DataFrame()
            self.output_stats = {}
        results.columns = [ind]
        self.output_results = self.output_results.join(results, how='outer')
        self.output_stats[ind] = stats
        ind += 1

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_features(self):
        return ['AdjClose', 'AdjVolume',
                'PRMA5_AdjClose', 'PRMA10_AdjClose',
                'BOLL10_AdjClose', 'BOLL20_AdjClose',
                'MFI10_AdjClose', 'MFI20_AdjClose',
                'VOL5_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose',
                'AvgDolVol', 'PRMA10_AvgDolVol',
                'RClose', 'RCashDividend',
                'SplitFactor', 'GSECTOR', 'EARNINGSFLAG']

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
    args = parser.parse_args()

    if args.data:
        StatArbStrategy().make_data()
    elif args.write_simulation:
        strategy = StatArbStrategy('version_0004', True)
        strategy.start()
    elif args.simulation:
        import pdb; pdb.set_trace()
        strategy = StatArbStrategy('version_0004', False)
        strategy.start()
