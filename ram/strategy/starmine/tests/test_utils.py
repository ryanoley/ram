import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.long_pead.utils import *


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

    def test_ern_date_blackout(self):
        result = ern_date_blackout(self.data, -1, 1)
        benchmark = np.array([0, 1, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 1)
        benchmark = np.array([0, 0, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 3)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)

    def test_ern_price_anchor(self):
        data = self.data.copy()
        data = ern_date_blackout(data, -1, 1)
        result = ern_price_anchor(data, 1, 3)
        benchmark = np.array([np.nan] * 4 + [4, 4, 5, 6.])
        benchmark = np.append(benchmark, benchmark * 10)
        assert_array_equal(result.anchor_price.values, benchmark)
        #
        result = ern_price_anchor(data, 0, 3)
        benchmark = np.array([np.nan] * 4 + [3, 4, 5, 6.])
        benchmark = np.append(benchmark, benchmark * 10)
        assert_array_equal(result.anchor_price.values, benchmark)
        #
        data2 = self.data.copy()
        data2 = ern_date_blackout(data2, -1, 1)
        data2['SecCode'] = ['1234'] * 16
        data2['Date'] = ['2015-01-01', '2015-01-02', '2015-01-03',
                         '2015-01-04', '2015-01-05', '2015-01-06',
                         '2015-01-07', '2015-01-08', '2015-01-09',
                         '2015-01-10', '2015-01-11', '2015-01-12',
                         '2015-01-13', '2015-01-14', '2015-01-15',
                         '2015-01-16']
        result = ern_price_anchor(data2, 1, 3)
        benchmark = np.array([np.nan] * 4 + [4, 4, 5, 6, 7.] +
                             [np.nan] * 3 + [40, 40, 50, 60.])
        assert_array_equal(result.anchor_price.values, benchmark)

    def test_make_anchor_ret_rank(self):
        data = self.data.copy()
        data = ern_date_blackout(data, -1, 1)
        result = make_anchor_ret_rank(data)

    def test_ern_return(self):
        result = ern_return(self.data)
        benchmark = [1.]*4 + [2.]*4
        benchmark = np.array(benchmark * 2)
        assert_array_equal(result.earnings_ret.values, benchmark)
        data2 = self.data.copy()
        data2 = ern_date_blackout(data2, -1, 1)
        data2['SecCode'] = ['1234'] * 16
        data2['Date'] = ['2015-01-01', '2015-01-02', '2015-01-03',
                         '2015-01-04', '2015-01-05', '2015-01-06',
                         '2015-01-07', '2015-01-08', '2015-01-09',
                         '2015-01-10', '2015-01-11', '2015-01-12',
                         '2015-01-13', '2015-01-14', '2015-01-15',
                         '2015-01-16']
        data2.loc[8:, 'AdjVwap'] = [9, 10, 12, 14, 15, 16, 17, 18]
        result = ern_return(data2)
        benchmark = np.array([1]*4 + [2]*8 + [1.4]*4)
        assert_array_equal(result.earnings_ret.values, benchmark)

    def test_outlier_rank(self):
        df = pd.DataFrame({
            'SecCode': ['a', 'a', 'a', 'b', 'b', 'b',
                        'c', 'c', 'c', 'd', 'd', 'd'],
            'Date': [1, 2, 3] * 4,
            'V1': [1, 2, 1, 2, 2, 1, 100, -100, 323, 3, 3, 3]
        })
        result = outlier_rank(df, 'V1', outlier_std=.4, pad=True)
        benchmark = df[['SecCode', 'Date', 'V1']]
        benchmark['V1'] = [0, 1/2., 1/6., 1/3., 1/2.,
                           1/6., 1, 0, 1, 2/3., 1, 2/3.]
        benchmark['V1_extreme'] = [0] * 6 + [1, -1, 1, 0, 0, 0]
        assert_frame_equal(result, benchmark)

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

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
