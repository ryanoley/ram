import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb2.portfolio import Portfolio


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
        result = portfolio.get_portfolio_stats()
        benchmark = {'min_ticket_charge_prc': 0.5}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
