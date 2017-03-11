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
            'AAPL~GOOGL': [2, 0, 4, 0],
            'AAPL~IBM': [0, 0, 3, 1],
            'GOOGL~IBM': [0, -2, -3, -2],
        }, index=dates)

        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'Date': dates * 3,
            'AdjClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RCashDividend': [0] * 12,
            'SplitFactor': [1] * 12,
            'EARNINGSFLAG': [0, 0, 0, 1] + [1, 0, 0, 0] + [0, 1, 0, 0]
        })
        self.pair_info = pd.DataFrame({})

    def test_set_and_prep_data(self):
        cons = PortfolioConstructor(booksize=200)
        cons.set_and_prep_data(self.scores, self.pair_info, self.data)
        result = cons.close_dict[pd.Timestamp('2015-01-01')]
        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9}
        self.assertDictEqual(result, benchmark)
        result = cons.close_dict[pd.Timestamp('2015-01-04')]
        benchmark = {'AAPL': 5, 'GOOGL': 15, 'IBM': 12}
        self.assertDictEqual(result, benchmark)
        result = cons.exit_scores[pd.Timestamp('2015-01-01')]
        benchmark = {'AAPL~GOOGL': 2, 'GOOGL~IBM': 0, 'AAPL~IBM': 0}
        self.assertDictEqual(result, benchmark)
        result = cons.exit_scores[pd.Timestamp('2015-01-04')]
        benchmark = {'AAPL~GOOGL': 0, 'GOOGL~IBM': -2, 'AAPL~IBM': 1}
        self.assertDictEqual(result, benchmark)

    def Xtest_get_daily_pl(self):
        cons = PortfolioConstructor(booksize=200)
        cons.set_and_prep_data(self.scores, self.pair_info, self.data)
        result, stats = cons.get_daily_pl(
            n_pairs=1, max_pos_prop=1, pos_perc_deviation=1, z_exit=1,
            remove_earnings=False)
        benchmark = pd.DataFrame({}, index=self.scores.index)
        benchmark['Ret'] = [-0.000500, 0.549125, -0.100000, -0.125375]
        assert_frame_equal(result, benchmark)

    def test_get_pos_exposures(self):
        port = PortfolioConstructor(booksize=200)
        trade_prices = {'IBM': 100, 'VMW': 200, 'GOOGL': 100, 'AAPL': 200}
        port._portfolio.add_pair(
            pair='IBM~VMW', trade_prices=trade_prices,
            gross_bet_size=10000, side=1)
        port._portfolio.add_pair(
            pair='IBM~GOOGL', trade_prices=trade_prices,
            gross_bet_size=10000, side=1)
        port._get_pos_exposures()
        result = port._exposures
        benchmark = {'IBM': 2, 'VMW': -1, 'GOOGL': -1}
        self.assertDictEqual(result, benchmark)

    def test_extract_earnings_signals(self):
        cons = PortfolioConstructor(booksize=200)
        cons.set_and_prep_data(self.scores, self.pair_info, self.data)
        cons._make_earnings_binaries()
        results = cons.earnings_flags

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
