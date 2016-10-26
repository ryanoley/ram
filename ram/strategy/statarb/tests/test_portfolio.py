import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.position import PairPosition
from ram.strategy.statarb.portfolio import PairPortfolio


class TestPairPortfolio(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        self.zscores = pd.DataFrame({
            'IBM_AAPL': [1, 2, 3],
            'IBM_VMW': [-1, -2, -3],
            'IBM_GOOGL': [1, 2, 3],
            'GOOGL_AAPL': [1, 2, 3]},
            index=pd.date_range('2010-01-01', periods=3))
        self.close = pd.DataFrame({
            'IBM': [100, 103, 106],
            'AAPL': [100, 100, 99],
            'VMW': [50, 52, 54],
            'GOOGL': [20, 21, 22]},
            index=pd.date_range('2010-01-01', periods=3))
        self.dividends = pd.DataFrame({
            'IBM': [0, 0, 0],
            'AAPL': [0, 0, 0],
            'VMW': [0, 0, 0],
            'GOOGL': [0, 0, 0]},
            index=pd.date_range('2010-01-01', periods=3))
        self.splits = pd.DataFrame({
            'IBM': [1, 1, 1],
            'AAPL': [1, 1, 1],
            'VMW': [1, 1, 1],
            'GOOGL': [1, 1, 1]},
            index=pd.date_range('2010-01-01', periods=3))

    def test_add_pairs(self):
        port = PairPortfolio()
        port.map_close_id_index(self.close)
        pairs = ['IBM_VMW', 'GOOGL_AAPL']
        sides = [1, -1]
        port.add_pairs(pairs, sides, self.close.iloc[0], 100000)
        self.assertListEqual(port.positions.keys(), ['IBM_VMW', 'GOOGL_AAPL'])

    def test_update_prices_daily_pl_gross_exposure(self):
        port = PairPortfolio()
        port.map_close_id_index(self.close)
        pairs = ['IBM_VMW', 'GOOGL_AAPL']
        sides = [1, -1]
        port.add_pairs(pairs, sides, self.close.iloc[0], 100000)
        result = port.get_gross_exposure()
        self.assertEqual(result, 100000)
        port.update_prices(self.close.iloc[1],
                           self.dividends.iloc[1],
                           self.splits.iloc[1])
        result = port.get_portfolio_daily_pl()
        self.assertEqual(result, -1500)
        port.update_prices(self.close.iloc[2],
                           self.dividends.iloc[2],
                           self.splits.iloc[2])
        result = port.get_portfolio_daily_pl()
        self.assertEqual(result, -1750)
        result = port.get_gross_exposure()
        self.assertEqual(result, 105750)

    def test_map_close_id_index(self):
        close = pd.DataFrame({'V1': [10, 11, 10, 9],
                              'V2': [10, 14, 20, 22],
                              'V3': [10, 13, 10, 4]})
        port = PairPortfolio()
        port.map_close_id_index(close)
        result = port.id_hash
        benchmark = {'V1': 0, 'V2': 1, 'V3': 2}
        self.assertDictEqual(result, benchmark)

    def test_get_close_prices(self):
        close = pd.DataFrame({'V1': [10, 11, 10, 9],
                              'V2': [10, 14, 20, 22],
                              'V3': [10, 13, 10, 4]})
        port = PairPortfolio()
        port.map_close_id_index(close)
        legs1 = ['V1', 'V1', 'V3', 'V3', 'V2']
        legs2 = ['V2', 'V1', 'V2', 'V3', 'V1']
        pairs = ['{0}_{1}'.format(l1, l2) for l1, l2 in zip(legs1, legs2)]
        result1, result2 = port._get_close_prices(pairs, close.iloc[1])
        benchmark = np.array([11, 11, 13, 13, 14])
        assert_array_equal(result1, benchmark)
        benchmark = np.array([14, 11, 14, 13, 11])
        assert_array_equal(result2, benchmark)

    def test_get_symbol_values(self):
        port = PairPortfolio()
        port.map_close_id_index(self.close)
        pairs = ['IBM_VMW', 'GOOGL_AAPL']
        sides = [1, -1]
        port.add_pairs(pairs, sides, self.close.iloc[0], 100000)
        result = port.get_gross_exposure()
        result = port.get_symbol_values()
        benchmark = {'VMW': -25000, 'AAPL': 25000,
                     'IBM': 25000, 'GOOGL': -25000}
        self.assertDictEqual(result, benchmark)
        port.update_prices(self.close.iloc[1],
                           self.dividends.iloc[1],
                           self.splits.iloc[1])
        result = port.get_symbol_values()
        benchmark = {'VMW': -26000, 'AAPL': 25000,
                     'IBM': 25750, 'GOOGL': -26250}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
