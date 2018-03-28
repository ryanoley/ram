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

    def test_update_prices_init_positions(self):
        portfolio = Portfolio()
        closes = {'AAPL': 10, 'IBM': 20}
        dividends = {'AAPL': 0, 'IBM': 0}
        splits = {'AAPL': 1, 'IBM': 1}
        portfolio.update_prices(closes, dividends, splits)
        self.assertEqual(len(portfolio.positions), 2)

    def test_update_position_sizes(self):
        portfolio = Portfolio()
        init_prices = {'AAPL': 10, 'IBM': 20, 'TSLA': 20}
        portfolio.update_prices(init_prices, dividends={}, splits={})
        result = portfolio.positions.keys()
        self.assertTrue('TSLA' in result)
        self.assertTrue('BAC' not in result)
        # Add new positions
        sizes = {'AAPL': 100, 'IBM': 100, 'BAC': 2000}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        portfolio.update_position_sizes(sizes, exec_prices)
        # No TSLA, added BAC to portfolio
        result = portfolio.positions.keys()
        self.assertTrue('TSLA' not in result)
        self.assertTrue('BAC' in result)
        self.assertEqual(portfolio.positions['AAPL'].shares, 10)
        self.assertEqual(portfolio.positions['IBM'].shares, 5)
        self.assertEqual(portfolio.positions['BAC'].shares, 20)

    def test_get_portfolio_exposure(self):
        portfolio = Portfolio()
        sizes = {'AAPL': -100, 'IBM': -100, 'BAC': 2000}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        portfolio.update_position_sizes(sizes, exec_prices)
        result = portfolio.get_portfolio_exposure()
        self.assertEqual(result, 2200)

    def test_get_portfolio_daily_pl(self):
        portfolio = Portfolio()
        sizes = {'AAPL': -100, 'IBM': -100, 'BAC': 2000}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        portfolio.update_position_sizes(sizes, exec_prices)
        # No changes, so to flush out commissions from first day
        closes = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        dividends = {'AAPL': 0, 'IBM': 0, 'BAC': 0}
        splits = {'AAPL': 1, 'IBM': 1, 'BAC': 1}
        portfolio.update_prices(closes, dividends, splits)
        portfolio.get_portfolio_daily_pl()
        # Everything goes up 10%
        closes = {'AAPL': 11, 'IBM': 22, 'BAC': 110}
        dividends = {'AAPL': 0, 'IBM': 0, 'BAC': 0}
        splits = {'AAPL': 1, 'IBM': 1, 'BAC': 1}
        portfolio.update_prices(closes, dividends, splits)
        result = portfolio.get_portfolio_daily_pl()
        self.assertEqual(result[0], 200)
        self.assertEqual(result[1], -20)

    def test_get_portfolio_daily_turnover(self):
        portfolio = Portfolio()
        sizes = {'AAPL': -100, 'IBM': -100, 'BAC': 2000}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        portfolio.update_position_sizes(sizes, exec_prices)
        result = portfolio.get_portfolio_daily_turnover()
        self.assertEqual(result, 2200)

    def test_get_portfolio_stats(self):
        portfolio = Portfolio()
        sizes = {'AAPL': -100, 'IBM': -100, 'BAC': 2000}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        portfolio.update_position_sizes(sizes, exec_prices)
        closes = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        dividends = {'AAPL': 0, 'IBM': 0, 'BAC': 0}
        splits = {'AAPL': 1, 'IBM': 1, 'BAC': 1}
        portfolio.update_prices(closes, dividends, splits)
        portfolio.get_portfolio_daily_pl()
        closes = {'AAPL': 11, 'IBM': 22, 'BAC': 110}
        dividends = {'AAPL': 0, 'IBM': 0, 'BAC': 0}
        splits = {'AAPL': 1, 'IBM': 1, 'BAC': 1}
        portfolio.update_prices(closes, dividends, splits)
        portfolio.get_portfolio_daily_pl()
        result = portfolio.get_portfolio_stats()
        benchmark = {'worst_losing_day_count': 2}
        self.assertDictEqual(result, benchmark)

    def test_close_portfolio_positions(self):
        portfolio = Portfolio()
        sizes = {'AAPL': -100, 'IBM': -100, 'BAC': 2000}
        exec_prices = {'AAPL': 10, 'IBM': 20, 'BAC': 100}
        portfolio.update_position_sizes(sizes, exec_prices)
        portfolio.close_portfolio_positions()
        result = max([x.exposure for x in portfolio.positions.values()])
        self.assertEqual(result, 0)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
