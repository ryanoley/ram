import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.abstract.portfolio import Portfolio


class TestPortfolio(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        pass

    def test_update_prices(self):
        portfolio = Portfolio()
        closes = {'AAPL': 10, 'IBM': 20}
        dividends = {'AAPL': 0, 'IBM': 0}
        splits = {'AAPL': 1, 'IBM': 1}
        portfolio.update_prices(closes, dividends, splits)

    def test_get_portfolio_stats(self):
        portfolio = Portfolio()
        closes = {'AAPL': 10, 'IBM': 20, 'TSLA': 20}
        dividends = {'AAPL': 0, 'IBM': 0, 'TSLA': 0}
        splits = {'AAPL': 1, 'IBM': 1, 'TSLA': 1}
        sizes = {'AAPL': 1000000, 'IBM': 400, 'TSLA': 0}
        portfolio.update_prices(closes, dividends, splits)
        portfolio.update_position_sizes(sizes, closes)

    def test_update_position_sizes(self):
        portfolio = Portfolio()
        sizes = {'AAPL': 10, 'IBM': 10}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'TSLA': 20}
        portfolio.update_prices(exec_prices, dividends={}, splits={})
        portfolio.update_position_sizes(sizes, exec_prices)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
