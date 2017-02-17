import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1


class TestPairsStrategy1(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_corr_coef(self):
        df = pd.DataFrame({
            'V1': range(1, 11),
            'V2': range(1, 11)[::-1],
            'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]}).pct_change().dropna()
        df_a = np.array(df)
        result = PairsStrategy1._get_corr_coef(df_a)
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
        result = PairsStrategy1._get_corr_moves(rets_a[:, 0], rets_a)
        benchmark = np.array([1, 0, 0.33333])
        assert_array_equal(result.round(5), benchmark)
        result = PairsStrategy1._get_corr_moves(rets_a[:, 1], rets_a)
        benchmark = np.array([0, 1, 0.66667])
        assert_array_equal(result.round(5), benchmark)

    def test_get_vol_ratios(self):
        df = pd.DataFrame({
            'V1': range(1, 11),
            'V2': range(1, 11)[::-1],
            'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]}).pct_change().dropna()
        df_a = np.array(df)
        result = PairsStrategy1._get_vol_ratios(df_a[:, 0], df_a)
        benchmark = np.array([1, 0.45779, 5.31404])
        assert_array_equal(result.round(5), benchmark)
        result = PairsStrategy1._get_vol_ratios(df_a[:, 1], df_a)
        benchmark = np.array([2.18442491, 1, 11.60811499])
        assert_array_equal(result.round(5), benchmark.round(5))

    def test_get_spread_zscores(self):
        prices = pd.DataFrame({'V1': [10, 12, 15, 14],
                               'V2': [21, 24, 25, 22]})
        close1 = prices.loc[:, ['V1', 'V2']]
        close2 = prices.loc[:, ['V2', 'V1']]
        pairs = PairsStrategy1()
        results = pairs._get_spread_zscores(close1, close2, 3)
        benchmark = pd.DataFrame(index=range(4))
        benchmark['V1'] = [np.nan, np.nan, 1.131308968, 0.795301976]
        benchmark['V2'] = [np.nan, np.nan, -1.131308968, -0.795301976]
        assert_frame_equal(results, benchmark)
        # Missing values in close prices
        prices = pd.DataFrame({'V1': [10, 12, 15, np.nan],
                               'V2': [21, 24, 25, 22]})
        close1 = prices.loc[:, 'V1']
        close2 = prices.loc[:, 'V2']
        results = pairs._get_spread_zscores(close1, close2, 3)
        benchmark = pd.Series([np.nan, np.nan, 1.131308968, np.nan],
                              name='V1')
        assert_series_equal(results, benchmark)

    def test_get_stats_all_pairs(self):
        pairs = PairsStrategy1()
        df = pd.DataFrame({'V1': range(1, 11), 'V2': range(1, 11)[::-1],
                           'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]})
        result = pairs._get_stats_all_pairs(df)
        self.assertListEqual(result.Leg1.tolist(), ['V1', 'V1', 'V2'])
        self.assertListEqual(result.Leg2.tolist(), ['V2', 'V3', 'V3'])

    def test_get_test_zscores(self):
        pairs = PairsStrategy1()
        dates = [dt.datetime(2015, 1, i) for i in range(1, 11)]
        df = pd.DataFrame({'V1': range(1, 11), 'V2': range(1, 11)[::-1],
                           'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]},
                          index=dates)
        cut_date = dt.datetime(2015, 1, 4)
        fpairs = pd.DataFrame({'Leg1': ['V2', 'V1'], 'Leg2': ['V1', 'V3']})
        result = pairs._get_test_zscores(df, cut_date, fpairs, window=2)

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
