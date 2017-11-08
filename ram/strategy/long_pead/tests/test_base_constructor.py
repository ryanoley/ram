import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.constructor.base_constructor import *


class ConstructorTest(Constructor):
    def get_args(self):
        return {'v1': [1, 2]}

    def get_position_sizes(self, scores):
        return scores


class TestBaseConstructor(unittest.TestCase):

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

    def test_filter_seccodes(self):
        data = {
            dt.date(2010, 1, 1): {'1': 4, '2': 5, '3': 0.1},
            dt.date(2010, 1, 2): {'1': 1, '2': 1, '3': 10}
        }
        date = dt.date(2010, 1, 1)
        result = filter_seccodes(data[date], 3)
        self.assertListEqual(result, ['3'])
        date = dt.date(2010, 1, 2)
        result = filter_seccodes(data[date], 3)
        self.assertListEqual(result, ['1', '2'])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
