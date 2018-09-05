import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

import ram.analysis.statistics as stats


class TestPairs(unittest.TestCase):

    def setUp(self):
        self.returns = pd.DataFrame({
            'V1': [0.1, -0.2, 0.3, -0.1, -0.04, -0.01],
            'V2': [0.4, -0.01, -0.01, -0.02, -0.02, -0.02]
        }, index=pd.date_range('2016-01-01', '2016-01-06'))

    def test_get_stats(self):
        result = stats.get_stats(self.returns)
        # Handful of spot checked values
        self.assertEqual(result.iloc[0, 0], 0.05)
        self.assertEqual(result.iloc[7, 1], -0.02)

    def test_get_draw_downs(self):
        result = stats._get_drawdowns(self.returns)
        benchmark = pd.DataFrame(index=['V1', 'V2'])
        benchmark['DD%'] = [-0.20, -0.08]
        benchmark['DDDays'] = [2, 5.]
        benchmark['UnderwaterDays'] = [3, 5.]
        benchmark['Underwater%'] = [-0.15, -0.08]
        assert_frame_equal(result, benchmark)

    def test_time_at_highs(self):
        result = stats._time_at_highs(self.returns)
        benchmark = pd.DataFrame(
            [1/3., 1/6.],
            index=['V1', 'V2'], columns=['TimeAtHighs']).round(3)
        assert_frame_equal(result, benchmark)

    def test_value_at_risk(self):
        result = stats._value_at_risk(self.returns)
        benchmark = pd.DataFrame(index=['V1', 'V2'])
        benchmark['VaR_5perc'] = [-0.20, -0.02]
        benchmark['VaR_1perc'] = [-0.20, -0.02]
        assert_frame_equal(result, benchmark)

    def test_c_value_at_risk(self):
        returns = pd.DataFrame({
            'V1': np.arange(100),
            'V2': np.arange(20, 120)
        })
        result = stats._c_value_at_risk(returns)
        benchmark = pd.DataFrame(
            [2.0, 22.0], index=['V1', 'V2'], columns=['CVaR_5perc'])
        assert_frame_equal(result, benchmark)

    def test_lower_partial_moment(self):
        result = stats._lower_partial_moment(self.returns)
        benchmark = pd.DataFrame(
            [8.6167, 0.2333], index=['V1', 'V2'], columns=['LPM_2'])
        assert_frame_equal(result, benchmark)

    def test_rollup_returns(self):
        data = pd.DataFrame(index=[dt.datetime(2010, 1, 1),
                                   dt.datetime(2010, 1, 2),
                                   dt.datetime(2010, 4, 1),
                                   dt.datetime(2010, 10, 1)])
        data['Rets'] = [10, 20, 30, 40]
        result = stats.rollup_returns(data)
        benchmark = pd.DataFrame(columns=[1, 2, 4],
                                 index=[2010])
        benchmark.iloc[0] = [30, 30, 40]
        benchmark.columns.name = 'Qtr'
        benchmark.index.name = 'Year'
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
