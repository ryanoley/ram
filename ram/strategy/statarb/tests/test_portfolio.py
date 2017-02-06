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
        trade_prices = {'IBM': 100, 'VMW': 200, 'GOOGL': 100, 'AAPL': 200}
        port = PairPortfolio()
        port.add_pair(pair='IBM_VMW',
                      trade_prices=trade_prices,
                      dollar_size=10000,
                      side=1)
        port.add_pair(pair='GOOGL_AAPL',
                      trade_prices=trade_prices,
                      dollar_size=10000,
                      side=1)
        self.port = port

    def test_add_pair(self):
        port = self.port
        self.assertListEqual(port.pairs.keys(), ['IBM_VMW', 'GOOGL_AAPL'])
        self.assertEqual(port.pairs['IBM_VMW'].shares1, 50)
        self.assertEqual(port.pairs['IBM_VMW'].shares2, -25)
        self.assertEqual(port.pairs['IBM_VMW'].gross_exposure, 10000)

    def test_close_pairs(self):
        port = self.port
        port.close_pairs(['IBM_VMW'])
        self.assertEqual(port.pairs.keys(), ['IBM_VMW', 'GOOGL_AAPL'])
        self.assertEqual(port.pairs['IBM_VMW'].gross_exposure, 0)
        self.assertNotEqual(port.pairs['GOOGL_AAPL'].gross_exposure, 0)
        self.assertEqual(port.count_open_positions(), 1)
        port.close_pairs(all_pairs=True)
        self.assertEqual(port.count_open_positions(), 0)
        self.assertEqual(port.pairs['IBM_VMW'].gross_exposure, 0)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
