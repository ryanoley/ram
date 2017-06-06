import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.constructor.constructor2 import PortfolioConstructor2


class TestConstructor2(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04']
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
            'EARNINGSFLAG': [0, 0, 0, 1] + [1, 0, 0, 0] + [0, 1, 0, 0],
            'TestFlag': [True] * 12
        })
        self.data.Date = convert_date_array(self.data.Date)
        self.pair_info = pd.DataFrame({'Leg1': ['AAPL', 'AAPL'],
                                       'Leg2': ['GOOGL', 'IBM']})

    def test_set_and_prep_data(self):
        cons = PortfolioConstructor2()
        cons.set_and_prep_data(self.data, self.pair_info, 1)
        result = cons.close_dict[dt.date(2015, 1, 1)]
        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9}
        self.assertDictEqual(result, benchmark)
        result = cons.close_dict[dt.date(2015, 1, 4)]
        benchmark = {'AAPL': 5, 'GOOGL': 15, 'IBM': 12}
        self.assertDictEqual(result, benchmark)
        # Add another index of data with new securities
        data2 = self.data.copy()
        data2['SecCode'] = ['AAPL'] * 4 + ['GOOGL'] * 4 + ['TSLA'] * 4
        pairs2 = pd.DataFrame({'Leg1': ['AAPL', 'AAPL'],
                               'Leg2': ['GOOGL', 'TSLA']})
        cons.set_and_prep_data(data2, pairs2, 2)
        result = cons.close_dict[dt.date(2015, 1, 1)]
        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9, 'TSLA': 9}
        self.assertDictEqual(result, benchmark)

    def test_get_zscores(self):
        cons = PortfolioConstructor2()
        close_data = pd.DataFrame({
            'AdjClose': [10, 12, 15, 14, 21, 24, 25, 22],
            'SecCode': ['V1'] * 4 + ['V2'] * 4,
            'Date': [dt.datetime(2015, 1, i) for i in [1, 2, 3, 4]] * 2
        })
        pair_info = pd.DataFrame({'Leg1': ['V1', 'V2'], 'Leg2': ['V2', 'V1']})
        results = cons._get_zscores(close_data, pair_info, window=3)
        benchmark = pd.DataFrame(
            index=[dt.datetime(2015, 1, i) for i in [1, 2, 3, 4]])
        benchmark.index.name = 'Date'
        benchmark['V1~V2'] = [np.nan, np.nan, 1.131308968, 0.795301976]
        benchmark['V2~V1'] = [np.nan, np.nan, -1.131308968, -0.795301976]
        assert_frame_equal(results, benchmark)

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

    def Xtest_set_and_prep_data(self):
        cons = PortfolioConstructor(booksize=200)
        cons.set_and_prep_data(self.scores, self.pair_info, self.data)
        cons._make_earnings_binaries()
        results = cons.earnings_flags

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
