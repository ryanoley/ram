import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.position_comb import MultiLegPosition


class TestMultiLegPosition(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        pass

    def test_init(self):
        pos = MultiLegPosition(
            np.array(['IBM', 'AAPL', 'PYPL', 'EXAS']),
            np.array([100, 100, 200, 50]),
            np.array([1000, 1000, -1000, -1000]))
        assert_array_equal(pos.shares, np.array([10, 10, -5, -20]))
        self.assertEqual(pos.gross_exposure, 4000)
        self.assertTrue(pos.open_position)
        self.assertFalse(pos.to_close_position)

    def test_update_position_prices(self):
        pos = MultiLegPosition(
            np.array(['IBM', 'AAPL', 'PYPL', 'EXAS']),
            np.array([100, 100, 200, 200]),
            np.array([1000, 1000, -1000, -1000]))
        prices = {'IBM': 101, 'AAPL': 101, 'PYPL': 205, 'EXAS': 205}
        pos.update_position_prices(prices)
        assert_array_equal(pos.prices_current, np.array([101, 101, 205, 205]))
        self.assertEqual(pos.daily_pl, -30)
        self.assertEqual(pos.gross_exposure, 4070)
        prices = {'IBM': 95, 'AAPL': 95, 'PYPL': 204, 'EXAS': 204}
        pos.update_position_prices(prices)
        assert_array_equal(pos.prices_current, np.array([95, 95, 204, 204]))
        self.assertEqual(pos.daily_pl, -110)
        self.assertEqual(pos.gross_exposure, 3940)
        prices = {'IBM': np.nan, 'AAPL': 95, 'PYPL': 202, 'EXAS': 202}
        pos.update_position_prices(prices)
        assert_array_equal(pos.prices_current, np.array([np.nan, 95,
                                                         202, 202]))
        assert_array_equal(pos.shares, np.array([0, 10, -5, -5.]))
        self.assertEqual(pos.daily_pl, 20)
        self.assertTrue(pos.to_close_position)

    def test_update_position_prices_splits_divs(self):
        pos = MultiLegPosition(
            np.array(['IBM', 'AAPL']),
            np.array([100, 200]),
            np.array([1000, -1000]))
        prices = {'IBM': 101, 'AAPL': 105}
        splits = {'IBM': 1, 'AAPL': 2}
        pos.update_position_prices(prices, splits=splits)
        assert_array_equal(pos.prices_entry, np.array([100, 100.]))
        assert_array_equal(pos.shares, np.array([10, -10.]))
        self.assertEqual(pos.daily_pl, -40)
        self.assertEqual(pos.gross_exposure, 2060)
        # Do dividends
        prices = {'IBM': 101, 'AAPL': 98}
        divs = {'IBM': 1, 'AAPL': 1}
        pos.update_position_prices(prices, dividends=divs)
        self.assertEqual(pos.daily_pl, 70)

    def test_update_position_exposure(self):
        pos = MultiLegPosition(
            np.array(['IBM', 'AAPL']),
            np.array([100, 200]),
            np.array([50000, -50000]))
        prices = {'IBM': 104, 'AAPL': 185}
        pos.update_position_prices(prices)
        prices = {'IBM': 101, 'AAPL': 175}
        pos.update_position_prices(prices)
        self.assertEqual(pos.gross_exposure, 94250)
        self.assertEqual(pos.net_exposure, 6750)

        pos.update_position_exposure(100000)
        self.assertEqual(pos.gross_exposure, 99870)
        self.assertEqual(pos.net_exposure, 120)
        self.assertEqual(pos.stat_rebalance_count, 1)

    def test_close_position(self):
        pos = MultiLegPosition(
            np.array(['IBM', 'AAPL']),
            np.array([100, 200]),
            np.array([1000, -1000]), 0.001)
        self.assertTrue(pos.open_position)
        self.assertEqual(pos.daily_pl, -0.015)
        pos.close_position()
        self.assertFalse(pos.open_position)
        self.assertEqual(pos.daily_pl, -0.03)

    def test_position_stats(self):
        pos = MultiLegPosition(
            np.array(['IBM', 'AAPL']),
            np.array([100, 200]),
            np.array([1000, -1000]), 0.001)
        prices = {'IBM': 104, 'AAPL': 200}
        pos.update_position_prices(prices)
        self.assertEqual(pos.stat_perc_gain, 0.02)
        self.assertEqual(pos.stat_holding_days, 1)
        prices = {'IBM': 96, 'AAPL': 200}
        pos.update_position_prices(prices)
        self.assertEqual(pos.stat_perc_gain, -0.02)
        self.assertEqual(pos.stat_holding_days, 2)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
