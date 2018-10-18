import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_frame_equal

from ram.strategy.sandbox.base.features import *


class TestFeatures(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 8
        data['Date'] = pd.date_range(dt.date(2015,1,1), dt.date(2015,1,8))
        data['SplitFactor'] = [.5, .5, .5, 1, 1, 1, 1, 1]
        data['AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]

        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        data = data.append(data2).reset_index(drop=True)
        data['AdjVwap'] = data['AdjClose'].copy()
        data['LEAD1_AdjVwap'] = data['AdjClose'].copy()
        data['LEAD11_AdjVwap'] = data['AdjClose'].copy() + \
                                    np.random.choice([1,-1], len(data))
        self.data = data

    def test_get_vwap_returns(self):
        data = self.data.copy()
        result = get_vwap_returns(data, 10)
        benchmark = (data.LEAD11_AdjVwap / data.LEAD1_AdjVwap) - 1
        assert_array_equal(benchmark.values, result.Ret10.values)

        mkt_data = data.copy()
        mkt_data['SecCode'] = 'HEDGE'
        mkt_data.drop_duplicates('Date', inplace=True)
        result = get_vwap_returns(data, 10, hedged=True, market_data=mkt_data)
        mkt_rets = (mkt_data.LEAD11_AdjVwap / mkt_data.LEAD1_AdjVwap) - 1
        benchmark =  benchmark.values - mkt_rets.append(mkt_rets).values
        assert_array_equal(benchmark, result.Ret10.values)

    def test_create_split_multiplier(self):
        result = create_split_multiplier(self.data)
        benchmark = self.data.drop('SplitFactor', axis=1)
        benchmark['SplitMultiplier'] = [1., 1., 1., 2., 1., 1., 1., 1.] * 2
        assert_frame_equal(benchmark, result)

    def test_n_day_high_low(self):
        test_data = self.data.copy()
        test_data['hl_col'] = [0, 2, 0, 2, 3, 0, 1, 1] * 2
        result = n_day_high_low(test_data, 'hl_col', 2, 'test')
        benchmark = test_data.copy()
        benchmark['test'] = [1, 1, 0, 1, 1, 0, 1, 1] * 2
        assert_frame_equal(benchmark, result)

        result = n_day_high_low(test_data, 'hl_col', 2, 'test', low=True)
        benchmark = test_data.copy()
        benchmark['test'] = [1, 0, 1, 0, 0, 1, 0, 1] * 2
        assert_frame_equal(benchmark, result)

    def test_two_var_signal(self):
        test_data = self.data.copy()
        test_data['bin_var'] = [1, 1, 1, 1, 0, 0, 0 ,0] * 2
        test_data['srt_var'] = range(16)
        binary_pivot = test_data.pivot(index='Date', columns='SecCode',
                                       values='bin_var')
        sort_pivot = test_data.pivot(index='Date', columns='SecCode',
                                     values='srt_var')
        result = two_var_signal(binary_pivot, sort_pivot, sort_pct=.5)
        benchmark = binary_pivot.copy()
        benchmark.iloc[:, 0] = [-1, -1, -1, -1, 0, 0, 0, 0]
        benchmark.iloc[:, 1] = [1, 1, 1, 1, 0, 0, 0, 0]
        assert_frame_equal(benchmark, result)

        binary_pivot['IBM'] = 1
        binary_pivot['AAPL'] = 1
        sort_pivot['IBM'] = 99
        sort_pivot['AAPL'] = 100
        result = two_var_signal(binary_pivot, sort_pivot, sort_pct=.5)
        benchmark = binary_pivot.copy()

        benchmark.iloc[:, 0] = [-1, -1, -1, -1, 0, 0, 0, 0]
        benchmark.iloc[:, 1] = [-1, -1, -1, -1, 0, 0, 0, 0]
        benchmark.iloc[:, 2] = [1, 1, 1, 1, -1, -1, -1, -1]
        benchmark.iloc[:, 3] = 1
        assert_frame_equal(benchmark, result)

        binary_pivot[:] = 0
        result = two_var_signal(binary_pivot, sort_pivot, sort_pct=.5)
        benchmark = binary_pivot.copy()
        assert_frame_equal(benchmark, result)

    def test_get_model_param_binaries(self):
        test_data = self.data.copy()
        test_data['bin_feature'] = ['a','b'] * 8
        result = get_model_param_binaries(test_data, ['bin_feature'])

        benchmark = test_data.copy()
        benchmark['attr_bin_feature_a'] = [True, False] * 8
        benchmark['attr_bin_feature_b'] = [False, True] * 8
        assert_frame_equal(benchmark, result)


    def test_n_pct_top_btm(self):
        result = n_pct_top_btm(self.data, 'AdjClose', 25, 'TestCol')
        benchmark = self.data.copy()
        benchmark['TestCol'] = [0] * 12 + [1] * 4
        benchmark.TestCol = benchmark.TestCol.astype(int)
        assert_frame_equal(benchmark, result)

        result = n_pct_top_btm(self.data, 'AdjClose', 25, 'TestCol', True)
        benchmark = self.data.copy()
        benchmark['TestCol'] = [1] * 4 + [0] * 12
        benchmark.TestCol = benchmark.TestCol.astype(int)
        assert_frame_equal(benchmark, result)


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

