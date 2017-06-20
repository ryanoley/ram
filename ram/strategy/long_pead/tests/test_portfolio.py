import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.long_pead.constructor.portfolio import Portfolio


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

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
