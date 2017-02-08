import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.position import PairPosition


class TestPairPosition(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        pass

    def test_init(self):
        pos = PairPosition('IBM', 100, 1000, 'AAPL', 200, -1000)
        self.assertEqual(pos.shares1, 10)
        self.assertEqual(pos.shares2, -5)
        self.assertEqual(pos.gross_exposure, 2000)
        self.assertTrue(pos.open_position)
        self.assertFalse(pos.to_close_position)

    def test_update_position_prices(self):
        pos = PairPosition('IBM', 100, 1000, 'AAPL', 200, -1000)
        pos.update_position_prices(101, 205)
        self.assertEqual(pos.p1, 101)
        self.assertEqual(pos.p2, 205)
        self.assertEqual(pos.daily_pl, -15)
        self.assertEqual(pos.gross_exposure, 2035)
        pos.update_position_prices(95, 204)
        self.assertEqual(pos.p1, 95)
        self.assertEqual(pos.p2, 204)
        self.assertEqual(pos.daily_pl, -55)
        self.assertEqual(pos.gross_exposure, 1970)
        pos.update_position_prices(np.nan, 202)
        self.assertTrue(np.isnan(pos.p1))
        self.assertEqual(pos.p2, 202)
        self.assertEqual(pos.daily_pl, 10)
        self.assertTrue(pos.to_close_position)
        self.assertEqual(pos.shares1, 0)
        self.assertEqual(pos.shares2, -5)

    def test_update_position_prices_splits_divs(self):
        pos = PairPosition('IBM', 100, 1000, 'AAPL', 200, -1000)
        # Do a split
        pos.update_position_prices(101, 105, 0, 0, 1, 2)
        self.assertEqual(pos.p2_entry, 100)
        self.assertTrue(pos.shares2, -10)
        self.assertEqual(pos.daily_pl, -40)
        self.assertEqual(pos.gross_exposure, 2060)
        # Do dividends
        pos.update_position_prices(101, 98, 1, 1, 1, 1)
        self.assertEqual(pos.daily_pl, 70)

    def test_update_position_exposure(self):
        pos = PairPosition('IBM', 100, 50000, 'AAPL', 200, -50000)
        pos.update_position_prices(104, 185)
        pos.update_position_prices(101, 175)
        self.assertEqual(pos.gross_exposure, 94250)
        self.assertEqual(pos.net_exposure, 6750)
        pos.update_position_exposure(100000)
        self.assertEqual(pos.gross_exposure, 99870)
        self.assertEqual(pos.net_exposure, 120)
        self.assertEqual(pos.stat_rebalance_count, 1)

    def test_close_position(self):
        pos = PairPosition('IBM', 100, 1000, 'AAPL', 200, -1000, 0.001)
        self.assertTrue(pos.open_position)
        self.assertEqual(pos.daily_pl, -0.015)
        pos.close_position()
        self.assertFalse(pos.open_position)
        self.assertEqual(pos.daily_pl, -0.03)

    def test_position_stats(self):
        pos = PairPosition('IBM', 100, 1000, 'AAPL', 200, -1000, 0)
        pos.update_position_prices(104, 200)
        self.assertEqual(pos.stat_perc_gain, 0.02)
        self.assertEqual(pos.stat_holding_days, 1)
        pos.update_position_prices(96, 200)
        self.assertEqual(pos.stat_perc_gain, -0.02)
        self.assertEqual(pos.stat_holding_days, 2)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
