import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.pairselector.pairs import PairSelector


class TestPairsSelector(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_corr_coef(self):
        df = pd.DataFrame({
            'V1': range(1, 11),
            'V2': range(1, 11)[::-1],
            'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]}).pct_change().dropna()
        df_a = np.array(df)
        result = PairSelector._get_corr_coef(df_a)
        benchmark = np.array([1., 0.58357289, 0.0937638])
        assert_array_equal(result[0].round(5), benchmark.round(5))
        benchmark = np.array([0.58357289, 1., 0.19369509])
        assert_array_equal(result[1].round(5), benchmark.round(5))

    def test_get_corr_moves(self):
        df = pd.DataFrame({
            'V1': range(1, 11),
            'V2': range(1, 11)[::-1],
            'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]}).pct_change().dropna()
        rets_a = np.array(df)
        result = PairSelector._get_corr_moves(rets_a[:, 0], rets_a)
        benchmark = np.array([1, 0, 0.33333])
        assert_array_equal(result.round(5), benchmark)
        result = PairSelector._get_corr_moves(rets_a[:, 1], rets_a)
        benchmark = np.array([0, 1, 0.66667])
        assert_array_equal(result.round(5), benchmark)

    def test_get_vol_ratios(self):
        df = pd.DataFrame({
            'V1': range(1, 11),
            'V2': range(1, 11)[::-1],
            'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]}).pct_change().dropna()
        df_a = np.array(df)
        result = PairSelector._get_vol_ratios(df_a[:, 0], df_a)
        benchmark = np.array([1, 0.45779, 5.31404])
        assert_array_equal(result.round(5), benchmark)
        result = PairSelector._get_vol_ratios(df_a[:, 1], df_a)
        benchmark = np.array([2.18442491, 1, 11.60811499])
        assert_array_equal(result.round(5), benchmark.round(5))

    def test_get_stats_all_pairs(self):
        pairs = PairSelector()
        df = pd.DataFrame({'V1': range(1, 11), 'V2': range(1, 11)[::-1],
                           'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]})
        result = pairs._get_stats_all_pairs(df)
        self.assertListEqual(result.Leg1.tolist(), ['V1', 'V1', 'V2'])
        self.assertListEqual(result.Leg2.tolist(), ['V2', 'V3', 'V3'])

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
