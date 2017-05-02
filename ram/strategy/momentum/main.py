import itertools
import pandas as pd

from ram.strategy.base import Strategy
from ram.strategy.momentum.constructor2 import MomentumConstructor2


class MomentumStrategy(Strategy):

    cons = MomentumConstructor2()

    def get_column_parameters(self):
        args1 = make_arg_iter(self.cons.get_iterable_args())
        output_params = {}
        for col_ind, x in enumerate(args1):
            output_params[col_ind] = x
        return output_params

    def run_index(self, time_index):
        data = self.read_data_from_index(time_index)
        args1 = make_arg_iter(self.cons.get_iterable_args())
        for arg_index, a1 in enumerate(args1):
            returns, stats = self.cons.get_daily_returns(
                data, self.get_date_parameters()['frequency'],
                arg_index, **a1)
            self._capture_output(returns, {'V1': 0}, arg_index)
        self.write_index_results(self.output_returns, time_index)
        self.write_index_stats(self.output_stats, time_index)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, returns, stats, arg_index):
        if arg_index == 0:
            self.output_returns = pd.DataFrame()
            self.output_stats = {}
        returns.columns = [arg_index]
        self.output_returns = self.output_returns.join(
            returns.iloc[:, 0], how='outer')
        self.output_stats[arg_index] = stats

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_features(self):
        return ['AdjClose', 'AdjVolume',
                'PRMA5_AdjClose', 'PRMA10_AdjClose',
                'MA5_AdjClose', 'MA30_AdjClose',
                'MA50_AdjClose', 'MA80_AdjClose',
                'MA120_AdjClose',
                'RClose', 'RCashDividend',
                'SplitFactor', 'GSECTOR', 'EARNINGSFLAG']

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
        MomentumStrategy().make_data()
    elif args.write_simulation:
        strategy = MomentumStrategy('version_0002', True)
        strategy.start()
    elif args.simulation:
        import pdb; pdb.set_trace()
        strategy = MomentumStrategy('version_0002', False)
        strategy.start()
