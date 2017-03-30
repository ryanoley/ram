import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.position import PairPosition


class TestPairPosition(unittest.TestCase):
    """
    Tests the implementation class PairPosition
    """
    def setUp(self):
        pass

    def test_init(self):
        pos = PairPosition('IBM', 'PYPL', 100, 200, 1000, -1000)
        self.assertEqual(pos.gross_exposure, 2000)
        self.assertTrue(pos.open_position)

    def test_update_position_prices(self):
        pos = PairPosition('IBM', 'PYPL', 100, 200, 1000, -1000)
        self.assertEqual(pos.daily_pl, -0.075)
        prices = {'IBM': 101, 'AAPL': 101, 'PYPL': 205, 'EXAS': 205}
        dividends = {'IBM': 0, 'AAPL': 0, 'PYPL': 0, 'EXAS': 0}
        splits = {'IBM': 1, 'AAPL': 1, 'PYPL': 1, 'EXAS': 1}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.prices_current1, 101)
        self.assertEqual(pos.prices_current2, 205)
        self.assertEqual(pos.shares1, 10)
        self.assertEqual(pos.shares2, -5)
        self.assertEqual(pos.daily_pl, -15)
        self.assertEqual(pos.gross_exposure, 2035)
        prices = {'IBM': 95, 'AAPL': 95, 'PYPL': 204, 'EXAS': 204}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.prices_current1, 95)
        self.assertEqual(pos.prices_current2, 204)
        self.assertEqual(pos.shares1, 10)
        self.assertEqual(pos.shares2, -5)
        self.assertEqual(pos.daily_pl, -55)
        self.assertEqual(pos.gross_exposure, 1970)
        prices = {'IBM': np.nan, 'AAPL': 95, 'PYPL': 202, 'EXAS': 202}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.daily_pl, 9.95)
        self.assertEqual(pos.gross_exposure, 1960)
        self.assertFalse(pos.open_position)

    def test_update_position_prices_splits_divs(self):
        pos = PairPosition('IBM', 'AAPL', 100, 200, 1000, -1000)
        prices = {'IBM': 101, 'AAPL': 105, 'PYPL': 205, 'EXAS': 205}
        dividends = {'IBM': 0, 'AAPL': 0, 'PYPL': 0, 'EXAS': 0}
        splits = {'IBM': 1, 'AAPL': 2, 'PYPL': 1, 'EXAS': 1}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.shares1, 10)
        self.assertEqual(pos.shares2, -10)
        self.assertEqual(pos.daily_pl, -40)
        self.assertEqual(pos.gross_exposure, 2060)
        # Do dividends
        prices = {'IBM': 101, 'AAPL': 98, 'PYPL': 205, 'EXAS': 205}
        dividends = {'IBM': 1, 'AAPL': 1, 'PYPL': 0, 'EXAS': 0}
        splits = {'IBM': 1, 'AAPL': 1, 'PYPL': 1, 'EXAS': 1}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.daily_pl, 70)
        self.assertEqual(pos.gross_exposure, 1990)

    def test_update_position_exposure(self):
        pos = PairPosition('IBM', 'AAPL', 100, 200, 50000, -50000)
        prices = {'IBM': 104, 'AAPL': 185}
        dividends = {'IBM': 0, 'AAPL': 0}
        splits = {'IBM': 1, 'AAPL': 1}
        pos.update_position_prices(prices, dividends, splits)
        prices = {'IBM': 101, 'AAPL': 175}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.gross_exposure, 94250)
        self.assertEqual(pos.net_exposure, 6750)

        pos.update_position_exposure(100000)
        self.assertEqual(pos.gross_exposure, 99870)
        self.assertEqual(pos.net_exposure, 120)
        self.assertEqual(pos.stat_rebalance_count, 1)

    def test_close_position(self):
        pos = PairPosition('IBM', 'AAPL', 100, 200, 1000, -1000, 0.001)
        self.assertTrue(pos.open_position)
        self.assertEqual(pos.daily_pl, -0.015)
        pos.close_position()
        self.assertFalse(pos.open_position)
        self.assertEqual(pos.daily_pl, -0.03)

    def test_position_stats(self):
        pos = PairPosition('IBM', 'AAPL', 100, 200, 1000, -1000, 0.001)
        prices = {'IBM': 104, 'AAPL': 200}
        dividends = {'IBM': 0, 'AAPL': 0}
        splits = {'IBM': 1, 'AAPL': 1}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.total_pl, 39.985)
        self.assertEqual(pos.stat_holding_days, 1)
        prices = {'IBM': 96, 'AAPL': 200}
        pos.update_position_prices(prices, dividends, splits)
        self.assertEqual(pos.total_pl, -40.015)
        self.assertEqual(pos.stat_holding_days, 2)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
