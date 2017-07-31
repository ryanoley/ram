import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.long_pead.constructor.constructor4 import *


class TestConstructor4(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04']
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
        self.data['Date'] = convert_date_array(self.data.Date)

    def test_get_position_sizes_with_groups(self):
        cons = PortfolioConstructor4()
        scores = {'AAPL': 4, 'IBM': 10, 'TSLA': -10, 'BAC': 4, 'GS': np.nan}
        groups = {'AAPL': 100, 'IBM': 100, 'TSLA': 200, 'BAC': 200, 'GS': 30}
        result = cons._get_position_sizes_with_groups(scores, groups, 1, 100)
        benchmark = {'AAPL': -13.163684327956279,
                     'IBM': 13.163684327956279,
                     'TSLA': -36.836315672043717,
                     'BAC': 36.836315672043717, 'GS': 0}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
