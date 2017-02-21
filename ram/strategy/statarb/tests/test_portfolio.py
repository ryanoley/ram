import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.portfolio import PairPortfolio


class TestPairPortfolio(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        trade_prices = {'IBM': 100, 'VMW': 200, 'GOOGL': 100, 'AAPL': 200}
        port = PairPortfolio()
        port.add_pair(pair='IBM~VMW',
                      trade_prices=trade_prices,
                      gross_bet_size=10000,
                      side=1)
        port.add_pair(pair='GOOGL~AAPL',
                      trade_prices=trade_prices,
                      gross_bet_size=10000,
                      side=1)
        self.port = port

    def test_add_pair(self):
        port = self.port
        self.assertListEqual(port.pairs.keys(), ['IBM~VMW', 'GOOGL~AAPL'])
        assert_array_equal(port.pairs['IBM~VMW'].shares, np.array([50, -25]))
        self.assertEqual(port.pairs['IBM~VMW'].gross_exposure, 10000)

    def test_close_pairs(self):
        port = self.port
        port.close_pairs(['IBM~VMW'])
        self.assertEqual(port.pairs.keys(), ['IBM~VMW', 'GOOGL~AAPL'])
        self.assertEqual(port.pairs['IBM~VMW'].gross_exposure, 0)
        self.assertNotEqual(port.pairs['GOOGL~AAPL'].gross_exposure, 0)
        self.assertEqual(port.count_open_positions(), 1)
        port.close_pairs(all_pairs=True)
        self.assertEqual(port.count_open_positions(), 0)
        self.assertEqual(port.pairs['IBM~VMW'].gross_exposure, 0)

    def test_get_period_stats(self):
        port = self.port
        closes = {'IBM': 103, 'VMW': 202, 'GOOGL': 110, 'AAPL': 198}
        dividends = {'IBM': 0, 'VMW': 0, 'GOOGL': 0, 'AAPL': 0}
        splits = {'IBM': 1, 'VMW': 1, 'GOOGL': 1, 'AAPL': 1}
        port.update_prices(closes, dividends, splits)
        port.close_pairs(['IBM~VMW'])
        port.get_portfolio_daily_pl()
        port.update_prices(closes, dividends, splits)
        port.close_pairs(all_pairs=True)
        port.get_portfolio_daily_pl()
        result = port.get_period_stats()
        benchmark = {
            'avg_perc_gain': 0.0325,
            'max_perc_gain': 0.055,
            'min_perc_gain': 0.01,
            'total_trades': 2,
            'avg_rebalance_count': 0.0,
            'max_holding_days': 2,
            'max_rebalance_count': 0,
            'avg_holding_days': 1.5
        }
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
