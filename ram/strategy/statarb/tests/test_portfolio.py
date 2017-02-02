import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.position import PairPosition
from ram.strategy.statarb.constructor.portfolio import PairPortfolio


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

    def test_add_pair(self):
        port = PairPortfolio()
        p1 = PairPosition('IBM', 100, 100000, 'VMW', 100, -100000)
        port.add_pair(p1)
        p2 = PairPosition('GOOGL', 100, 100000, 'AAPL', 100, -100000)
        port.add_pair(p2)
        self.assertListEqual(port.pairs.keys(), ['IBM_VMW', 'GOOGL_AAPL'])

    def test_get_symbol_values(self):
        port = PairPortfolio()
        p1 = PairPosition('IBM', 100, 100000, 'VMW', 100, -100000)
        port.add_pair(p1)
        p2 = PairPosition('GOOGL', 100, 100000, 'AAPL', 100, -100000)
        port.add_pair(p2)
        result = port.get_gross_exposure()
        result = port.get_symbol_counts()
        benchmark = {'VMW': -1, 'AAPL': -1,
                     'IBM': 1, 'GOOGL': 1}
        self.assertDictEqual(result, benchmark)
        p3 = PairPosition('GOOGL', 100, 100000, 'VMW', 100, -100000)
        port.add_pair(p3)
        result = port.get_symbol_counts()
        benchmark = {'VMW': -2, 'AAPL': -1,
                     'IBM': 1, 'GOOGL': 2}
        self.assertDictEqual(result, benchmark)
        p4 = PairPosition('VMW', 100, 100000, 'IBM', 100, -100000)
        port.add_pair(p4)
        result = port.get_symbol_counts()
        benchmark = {'VMW': -1, 'AAPL': -1,
                     'IBM': 0, 'GOOGL': 2}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
