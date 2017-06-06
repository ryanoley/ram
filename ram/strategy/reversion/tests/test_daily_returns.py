import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.reversion.daily_returns import get_daily_returns


class TestDailyReturns(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1), dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3), dt.datetime(2015, 1, 4),
                 dt.datetime(2015, 1, 5), dt.datetime(2015, 1, 6)]
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6 + ['GOOGL'] * 6 + ['IBM'] * 6,
            'AdjOpen': [10, 12, 11, 10, 9, 6, 10, 8, 4,
                        10, 11, 30, 20, 20, 20, 30, 32, 34],
            'AdjClose': [11, 13, 12, 12, 10, 6.5, 11, 9, 4.5,
                         13, 12, 32, 22, 20, 20, 34, 35, 36],
            'AdjVwap': [10.5, 12.5, 11.5, 11, 9.5, 6.25, 10.5, 8.5, 4.25, 11.5,
                        11.5, 31, 21, 20, 20, 32, 33.5, 35],
            'GGROUP': [10] * 18,
            'EARNINGSFLAG': [0] * 18,
            'TestFlag': [True] * 18
        })
        self.data['Date'] = dates * 3

    def test_get_daily_returns(self):
        result = get_daily_returns(self.data, feature_ndays=2,
                                   holding_ndays=2, n_per_side=1)
        benchmark = pd.DataFrame({
            'Ret': [0, 0, 0.007673, 0.958649, -0.051565, -0.187500]},
            index=[pd.Timestamp(2015, 1, i) for i in range(1, 7)])
        benchmark.index.name = 'Date'
        #assert_array_almost_equal(result.round(4).values, benchmark.round(4).values)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
