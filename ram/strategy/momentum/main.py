import pandas as pd

from ram.strategy.base import Strategy
from ram.strategy.momentum.constructor import MomentumConstructor


class MomentumStrategy(Strategy):

    cons = MomentumConstructor()

    def get_column_parameters(self):
        return {'V1': 0}

    def run_index(self, time_index):
        data = self.read_data_from_index(time_index)
        returns, stats = self.cons.get_daily_returns(data)
        self.write_index_results(returns, time_index)
        self.write_index_stats(stats, time_index)

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
            'test_period_length': 4,
            'start_year': 2002
        }


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
        strategy = MomentumStrategy('version_0002', False)
        strategy.start()
