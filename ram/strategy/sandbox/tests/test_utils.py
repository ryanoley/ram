import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.sandbox.base.utils import *
from pandas.util.testing import assert_frame_equal


class TestUtils(unittest.TestCase):

    def setUp(self):
        dates = [dt.date(2015,1,1), dt.date(2015,1,2), dt.date(2015,1,3),
                     dt.date(2015,1,4)]
        data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'Date': dates * 3,
            'AdjClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RCashDividend': [0] * 12,
            'SplitFactor': [1] * 12,
            'EARNINGSFLAG': [0, 0, 0, 1] + [1, 0, 0, 0] + [0, 1, 0, 0],
            'TestFlag': [True] * 12
        })
        self.data = data

    def test_make_variable_dict(self):
        test_data = self.data.copy()
        result = make_variable_dict(test_data, 'AdjClose')

        benchmark = {'AAPL': 5, 'GOOGL': 18, 'IBM': 11}
        self.assertDictEqual(result[dt.date(2015, 1, 3)], benchmark)

        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9}
        self.assertDictEqual(result[dt.date(2015, 1, 1)], benchmark)

        test_data = pd.DataFrame({
            'SecCode': ['a'] * 3 + ['b'] * 3,
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'] * 2,
            'V1': range(6),
            'V2': range(1, 7)
        })
        test_data.iloc[2, 2] = np.nan
        test_data.iloc[5, 3] = np.nan
        result = make_variable_dict(test_data, 'V1', fillna=np.nan)
        self.assertTrue(np.isnan(result['2010-01-03']['a']))
        self.assertEqual(result['2010-01-03']['b'], 5.0)

        result = make_variable_dict(test_data, 'V2', fillna=-99.0)
        self.assertFalse(np.isnan(result['2010-01-03']['b']))
        self.assertEqual(result['2010-01-03']['a'], 3.0)
        self.assertEqual(result['2010-01-03']['b'], -99.0)

    def test_append_col(self):
        test_data = self.data.copy()
        test_data.rename(columns={'SecCode':'Model'}, inplace=True)
        data_pivot = test_data.pivot(index='Date', columns='Model',
                                     values='RClose')
        data_pivot = data_pivot.rolling(2, min_periods=1).sum()
        result = append_col(test_data, data_pivot, 'test_col')
        benchmark = test_data.copy()
        benchmark['test_col'] = [10., 19, 14, 10, 10, 30, 38,
                                        33, 9, 19, 21, 23]
        assert_frame_equal(benchmark, result)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
