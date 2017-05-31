import itertools
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.strategy.long_pead.constructor.constructor import PortfolioConstructor


class LongPeadStrategy(Strategy):

    constructor = PortfolioConstructor()

    def get_column_parameters(self):
        args1 = make_arg_iter(self.constructor.get_data_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())
        output_params = {}
        for col_ind, (x, y) in enumerate(itertools.product(args1, args2)):
            params = dict(x)
            params.update(y)
            output_params[col_ind] = params
        return output_params

    def run_index(self, time_index):
        data = self.read_data_from_index(time_index)
        args = make_arg_iter(self.constructor.get_iterable_args())
        argsd = make_arg_iter(self.constructor.get_data_args())
        i = 0
        for ad in argsd:
            if ad['blackout_offset2'] < ad['anchor_init_offset']:
                continue
            self.constructor.set_and_prep_data(data, time_index, **ad)
            for a1 in args:
                result = self.constructor.get_daily_pl(arg_index=i, **a1)
                self._capture_output(result, i)
                i += 1
        returns = self.output_pl / self.constructor.booksize
        self.write_index_results(returns, time_index)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, arg_index):
        if arg_index == 0:
            self.output_pl = pd.DataFrame()
            self.output_exposure = pd.DataFrame()
        results = results[['PL', 'Exposure']].copy()
        results.columns = [arg_index, arg_index]
        self.output_pl = self.output_pl.join(results.iloc[:, 0], how='outer')
        self.output_exposure = self.output_exposure.join(
            results.iloc[:, 1], how='outer')

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR in (25) ' +
            'and Close_ between 10 and 500',
            'univ_size': 500}

    def get_features(self):
        return ['AdjClose', 'AvgDolVol',
                'RClose', 'RCashDividend', 'SplitFactor',
                'PRMA5_AdjClose', 'GGROUP', 'EARNINGSFLAG']

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 4,
            'test_period_length': 2,
            'start_year': 2002
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
        LongPeadStrategy().make_data()
    elif args.write_simulation:
        strategy = LongPeadStrategy('version_0005', True)
        strategy.start()
    elif args.simulation:
        import pdb; pdb.set_trace()
        strategy = LongPeadStrategy('version_0005', False)
        strategy.start()
