import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.birds.constructor.position import Position


class TestPosition(unittest.TestCase):

    def setUp(self):
        pass

    def test_init(self):
        pos = Position(symbol='IBM', entry_price=100, size=10000)
        self.assertEqual(pos.shares, 100)
        self.assertEqual(pos.exposure, 10000)
        self.assertTrue(pos.open_position)

    def test_update_position_price(self):
        pos = Position(symbol='IBM', entry_price=100, size=10000)
        prices = {'IBM': 101, 'AAPL': 100}
        pos.update_position_price(prices)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
