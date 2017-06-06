import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.constructor.constructor3 import PortfolioConstructor3


class TestConstructor3(unittest.TestCase):

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
        cons = PortfolioConstructor3()
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

    def Xtest_get_daily_pl(self):
        cons = PortfolioConstructor3(booksize=200)
        cons.set_and_prep_data(self.scores, self.pair_info, self.data)
        result, stats = cons.get_daily_pl(
            n_pairs=1, max_pos_prop=1, pos_perc_deviation=1, z_exit=1,
            remove_earnings=False)
        benchmark = pd.DataFrame({}, index=self.scores.index)
        benchmark['Ret'] = [-0.000500, 0.549125, -0.100000, -0.125375]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
