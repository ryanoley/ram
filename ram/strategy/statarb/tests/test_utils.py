import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.utils import *


class TestUtils(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04',
                 '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08']
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 8
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['MA5_AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0, 0, 0]
        data['TestFlag'] = [True] * 8
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        data = data.append(data2).reset_index(drop=True)
        data['AdjVwap'] = data['AdjClose'].copy()
        self.data = data

    def test_make_arg_iter(self):
        parameters = {'V1': [1, 2], 'V2': [3, 4]}
        result = make_arg_iter(parameters)
        benchmark = [{'V1': 1, 'V2': 3}, {'V1': 1, 'V2': 4},
                     {'V1': 2, 'V2': 3}, {'V1': 2, 'V2': 4}]
        self.assertListEqual(result, benchmark)

    def test_make_variable_dict(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04']
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
        data['Date'] = convert_date_array(data.Date)
        result = make_variable_dict(data, 'AdjClose')
        benchmark = {'AAPL': 5, 'GOOGL': 18, 'IBM': 11}
        self.assertDictEqual(result[dt.date(2015, 1, 3)], benchmark)
        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9}
        self.assertDictEqual(result[dt.date(2015, 1, 1)], benchmark)
        # Second test
        data = pd.DataFrame({
            'SecCode': ['a'] * 3 + ['b'] * 3,
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'] * 2,
            'V1': range(6),
            'V2': range(1, 7)
        })
        data.iloc[2, 2] = np.nan
        data.iloc[5, 3] = np.nan
        result = make_variable_dict(data, 'V1', fillna=np.nan)
        self.assertTrue(np.isnan(result['2010-01-03']['a']))
        self.assertEqual(result['2010-01-03']['b'], 5.0)
        result = make_variable_dict(data, 'V2', fillna=-99.0)
        self.assertFalse(np.isnan(result['2010-01-03']['b']))
        self.assertEqual(result['2010-01-03']['a'], 3.0)
        self.assertEqual(result['2010-01-03']['b'], -99.0)

    def test_responses(self):
        result = simple_responses(self.data, 2)
        benchmark = self.data[['SecCode', 'Date']].copy()
        benchmark = benchmark.sort_values(['Date', 'SecCode'])
        benchmark = benchmark.reset_index(drop=True)
        benchmark['Response'] = [1] * 12 + [0] * 4
        benchmark['TestFlag'] = self.data.TestFlag
        assert_frame_equal(result, benchmark)
        result = smoothed_responses(self.data, .25, days=[1, 2])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
