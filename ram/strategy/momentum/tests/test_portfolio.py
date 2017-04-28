import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.momentum.portfolio import Portfolio


class TestMomentumConstructor(unittest.TestCase):

    def setUp(self):
        port = Portfolio()
        alphas = {'A': 1, 'B': 1, 'C': -1, 'D': -1}
        prices = {'A': 100, 'B': 2, 'C': 10, 'D': 20}
        port.update_prices(prices)
        port.update_positions(alphas, prices)
        self.port = port

    def test_update_position(self):
        result = self.port._positions
        benchmark = {'A': 2500000.0, 'C': -2500000.0,
                     'B': 2500000.0, 'D': -2500000.0}
        self.assertDictEqual(result, benchmark)

    def test_update_prices(self):
        prices = {'A': 101, 'B': 3, 'C': 11, 'D': 21}
        benchmark = 0.09
        result = self.port.update_prices(prices)
        self.assertAlmostEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
