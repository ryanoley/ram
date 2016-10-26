import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.statistics import *


class TestPairs(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        self.daily_pl = pd.DataFrame({
            'V1': [1, -1, 2, -2],
            'V2': [10, -3, 5, -1]
        })

    def Xtest_aggregate_stats(self):

        result = aggregate_stats(self.daily_pl, 100)

        benchmark = pd.DataFrame(index=['V1', 'V2'])
        benchmark['UpDays'] = [8, 8]
        benchmark['DownDays'] = [4, 4]
        benchmark['FlatDays'] = [0, 0]
        benchmark['DD_Deepest_Loss'] = [-10.0, -14.0]
        benchmark['DD_Deepest_Days'] = [2, 6.]
        benchmark['DD_Longest_Loss'] = [-3.0, -14.0]
        benchmark['DD_Longest_Days'] = [4, 6.]
        benchmark['TimeAtHighs'] = [0.5, 0.5]
        benchmark['AvgExposure'] = [1.055, 1.055]
        benchmark['TotalPL'] = [13, 122]
        assert_frame_equal(benchmark, results)

    def Xtest_aggregate_stats(self):
        aggregate_stats(daily_pl, booksize=1e7)

    def test_get_up_down_flat_days(self):
        result = get_up_down_flat_days(self.daily_pl)
        benchmark = pd.DataFrame(index=['V1', 'V2'],
                                 columns=['UpDays', 'DownDays'])
        benchmark.iloc[0] = [2, 2]
        benchmark.iloc[1] = [2, 2]
        assert_frame_equal(result, benchmark)

    def test_get_draw_downs(self):
        result = get_draw_downs(self.daily_pl.cumsum())
        benchmark = pd.DataFrame(index=['V1', 'V2'])
        benchmark['DD_Deepest_Loss'] = [-2, -3.]
        benchmark['DD_Deepest_Days'] = [1, 2.]
        benchmark['DD_Longest_Loss'] = [-1, -3.]
        benchmark['DD_Longest_Days'] = [2, 2.]
        benchmark['DD_Average'] = [-1.5, -2.]
        assert_frame_equal(result, benchmark)

    def test_get_time_at_highs(self):
        result = get_time_at_highs(self.daily_pl.cumsum())
        benchmark = pd.Series([.5, .5], index=['V1', 'V2'], name='TimeAtHighs')
        assert_series_equal(result, benchmark)

    def test_volatility(self):
        rets = self.daily_pl / 100
        result = volatility(rets)
        benchmark = pd.Series([0.018257, 0.059090],
                              index=['V1', 'V2'], name='Volatility')
        assert_series_equal(result.round(6), benchmark)

    def test_value_at_risk(self):
        rets = self.daily_pl / 100
        result = value_at_risk(rets, alpha=0.25)
        benchmark = pd.Series([-0.01, -0.01], index=['V1', 'V2'],
                              name='VaR_5perc')
        assert_series_equal(result, benchmark)

    def test_c_value_at_risk(self):
        rets = self.daily_pl / 100
        result = c_value_at_risk(rets, alpha=0.5)
        benchmark = pd.Series([-.015, -.02], index=['V1', 'V2'],
                              name='CVaR_5perc')
        assert_series_equal(result, benchmark)

    def test_lower_partial_moment(self):
        rets = self.daily_pl / 100
        result = lower_partial_moment(rets, threshold=0, order=1)
        benchmark = pd.Series([0.0075, 0.01], index=['V1', 'V2'], name='LPM_1')
        assert_series_equal(result, benchmark)

    def test_sharpe_ratio(self):
        rets = self.daily_pl / 100
        result = sharpe_ratio(rets, index=None)
        benchmark = pd.Series([0, 0.465389], index=['V1', 'V2'], name='Sharpe')
        assert_series_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
