import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb2.position import Position


class TestPosition(unittest.TestCase):
    """
    Tests the implementation class PairPosition
    """
    def setUp(self):
        pass

    def test_init(self):
        pos = Position(symbol='IBM', price=100)
        pos.update_position_size(-10004, 100)
        self.assertEqual(pos.shares, -100)
        self.assertTrue(pos.open_position)
        self.assertTrue(pos.daily_pl, -.5)

    def test_update_position_prices(self):
        pos = Position(symbol='IBM', price=100)
        pos.update_position_size(-10004, 100)
        pos.get_daily_pl()
        pos.update_position_prices(price=101, dividend=0, split=1)
        self.assertEqual(pos.get_daily_pl(), -100.0)
        pos.update_position_prices(price=95, dividend=0, split=1)
        self.assertEqual(pos.get_daily_pl(), 600.0)
        pos.update_position_prices(price=96, dividend=1, split=1)
        self.assertEqual(pos.get_daily_pl(), -200.0)
        pos.update_position_prices(price=48, dividend=0, split=2)
        self.assertEqual(pos.shares, -200)
        self.assertEqual(pos.get_daily_pl(), 0.0)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
