import pandas as pd

from ram.strategy.base import Strategy
from ram.strategy.long_pead.utils import ern_date_blackout


class LongPeadStrategy(Strategy):

    def get_column_parameters(self):
        return []

    def run_index(self, time_index):
        data = self.read_data_from_index(time_index)
        # Anchor prices
        data = ern_date_blackout(data, offset1=-1, offset2=2)
        data = ern_price_anchor(data, offset=1)

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR in (25) ' +
            'and Close_ between 15 and 500',
            'univ_size': 500}

    def get_features(self):
        return ['AdjClose', 'AvgDolVol', 'PRMA5_AdjClose',
                'GGROUP', 'EARNINGSFLAG']

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 4,
            'test_period_length': 2,
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
        LongPeadStrategy().make_data()
    elif args.write_simulation:
        strategy = LongPeadStrategy('version_0001', True)
        strategy.start()
    elif args.simulation:
        import pdb; pdb.set_trace()
        strategy = LongPeadStrategy('version_0001', False)
        strategy.start()
