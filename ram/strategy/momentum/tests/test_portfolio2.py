import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.momentum.portfolio2 import Portfolio2


class TestMomentumConstructor(unittest.TestCase):

    def setUp(self):
        pass

    def test_add_positions(self):
        port = Portfolio2(2, 4)
        prices = {'A': 100, 'B': 2, 'C': 10, 'D': 20}
        signals = {'A': 1, 'B': 1, 'C': -1, 'D': -1}
        port.add_positions(port_id=0, signals=signals, prices=prices)
        self.assertEqual(len(port.portfolios[0]), 4)
        port.close_portfolio(0)
        self.assertEqual(len(port.portfolios[0]), 0)

    def test_get_daily_return(self):
        port = Portfolio2(2, 4)
        prices = {'A': 100, 'B': 20, 'C': 10, 'D': 20}
        signals = {'A': 1, 'B': 1, 'C': -1, 'D': -1}
        port.add_positions(port_id=0, signals=signals, prices=prices)
        prices = {'A': 101, 'B': 22, 'C': 15, 'D': 18}
        port.update_prices(prices)
        result = port.get_daily_return()
        self.assertAlmostEqual(result, -0.03625)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
