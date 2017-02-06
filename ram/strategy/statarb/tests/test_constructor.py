import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class TestConstructor(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1), dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3), dt.datetime(2015, 1, 4)]
        self.scores = pd.DataFrame({
            'AAPL_GOOGL': [2, 0, 4, 0],
            'AAPL_IBM': [0, 0, 3, 1],
            'GOOGL_IBM': [0, -2, -3, -2],
        }, index=dates)

        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'Date': dates * 3,
            'AdjClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RCashDividend': [0] * 12,
            'SplitFactor': [1] * 12
        })
        self.pair_info = pd.DataFrame({})

    def test_get_daily_pl(self):
        port = PortfolioConstructor(booksize=200)
        result = port.get_daily_pl(self.scores, self.data,
                                   self.pair_info, n_pairs=1,
                                   max_pos_prop=1)
        benchmark = pd.DataFrame({}, index=self.scores.index)
        benchmark['PL'] = [-0.06, 109.895, -20, -25.045]
        benchmark['Exposure'] = [200, 200, 200, 0.]
        assert_frame_equal(result, benchmark)

    def test_get_pos_exposures(self):
        port = PortfolioConstructor(booksize=200)
        trade_prices = {'IBM': 100, 'VMW': 200, 'GOOGL': 100, 'AAPL': 200}
        port._portfolio.add_pair(
            pair='IBM_VMW', trade_prices=trade_prices,
            dollar_size=10000, side=1)
        port._portfolio.add_pair(
            pair='IBM_GOOGL', trade_prices=trade_prices,
            dollar_size=10000, side=1)
        port._get_pos_exposures()
        result = port._exposures
        benchmark = {'IBM': 2, 'VMW': -1, 'GOOGL': -1}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
