import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.reversion.daily_returns import get_daily_returns


class TestDailyReturns(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1), dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3), dt.datetime(2015, 1, 4)]
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'AdjOpen': [10, 9, 5, 5] + [10, 14, 18, 14] + [9.5, 10, 11.4, 12],
            'AdjClose': [11, 10, 4, 6] + [8, 15, 21, 15] + [9, 11.2, 11.8, 13],
            'AdjVwap': [10.5, 9.5, 4.5, 5.5] + [9, 14.5, 20, 15.2] + [9.1, 11, 11.6, 12.3],
            'GGROUP': [10] * 12,
            'EARNINGSFLAG': [0, 0, 0, 1] + [1, 0, 0, 0] + [0, 1, 0, 0],
            'TestFlag': [False, True, True, True] * 3
        })
        self.data['Date'] = dates * 3

    def test_get_daily_returns(self):
        import pdb; pdb.set_trace()
        result = get_daily_returns(self.data, feature_ndays=1,
                                   holding_ndays=2, n_per_side=1)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
