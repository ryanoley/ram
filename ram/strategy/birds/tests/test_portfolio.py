import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.birds.constructor.portfolio import Portfolio


class TestPortfolio(unittest.TestCase):

    def setUp(self):
        trade_prices = {'IBM': 100, 'VMW': 200, 'GOOGL': 100, 'AAPL': 200}
        port = Portfolio()
        port.add_position(symbol='IBM',
                          trade_prices=trade_prices,
                          bet_size=10000)
        port.add_position(symbol='GOOGL',
                          trade_prices=trade_prices,
                          bet_size=-10000)
        self.port = port

    def test_add_position(self):
        port = self.port
        self.assertListEqual(port.positions.keys(), ['IBM', 'GOOGL'])
        self.assertEqual(port.positions['IBM'].shares, 100)
        self.assertEqual(port.positions['GOOGL'].shares, -100)

    def test_close_positions(self):
        port = self.port
        port.close_positions(['IBM'])
        self.assertEqual(port.positions.keys(), ['IBM', 'GOOGL'])
        self.assertEqual(port.positions['IBM'].exposure, 0)
        self.assertNotEqual(port.positions['GOOGL'].exposure, 0)
        port.close_all_positions()
        result = sum([x.open_position for x in port.positions.values()])
        self.assertEqual(result, 0)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
