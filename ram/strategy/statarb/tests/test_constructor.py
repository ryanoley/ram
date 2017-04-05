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

    def Xtest_get_test_zscores(self):
        pairs = PairSelector()
        dates = [dt.datetime(2015, 1, i) for i in range(1, 11)]
        df = pd.DataFrame({'V1': range(1, 11), 'V2': range(1, 11)[::-1],
                           'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]},
                          index=dates)
        cut_date = dt.datetime(2015, 1, 4)
        fpairs = pd.DataFrame({'Leg1': ['V2', 'V1'], 'Leg2': ['V1', 'V3']})
        result = pairs._get_test_zscores(df, cut_date, fpairs, window=2)

    def Xtest_get_spread_zscores(self):
        prices = pd.DataFrame({'V1': [10, 12, 15, 14],
                               'V2': [21, 24, 25, 22]})
        close1 = prices.loc[:, ['V1', 'V2']]
        close2 = prices.loc[:, ['V2', 'V1']]
        pairs = PairSelector()
        results = pairs._get_spread_zscores(close1, close2, 3)
        benchmark = pd.DataFrame(index=range(4))
        benchmark['V1'] = [np.nan, np.nan, 1.131308968, 0.795301976]
        benchmark['V2'] = [np.nan, np.nan, -1.131308968, -0.795301976]
        assert_frame_equal(results, benchmark)
        # Missing values in close prices
        prices = pd.DataFrame({'V1': [10, 12, 15, np.nan],
                               'V2': [21, 24, 25, 22]})
        close1 = prices.loc[:, 'V1']
        close2 = prices.loc[:, 'V2']
        results = pairs._get_spread_zscores(close1, close2, 3)
        benchmark = pd.Series([np.nan, np.nan, 1.131308968, np.nan],
                              name='V1')
        assert_series_equal(results, benchmark)

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
