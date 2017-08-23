import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.long_pead.constructor.constructor2 import *


class TestConstructor2(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_position_sizes(self):
        cons = PortfolioConstructor2()
        cons.market_cap = {'AAPL': 10, 'IBM': 20, 'BAC': 30, 'GS': 50}
        scores = {'AAPL': 4, 'IBM': 10, 'BAC': 4, 'GS': -10}
        result = cons.get_position_sizes(scores, 0.1, 'MarketCap', 2)
        benchmark = {'AAPL': -0.25, 'GS': -0.25, 'IBM': 0.25, 'BAC': 0.25}
        self.assertDictEqual(result, benchmark)
        cons.sector = {'AAPL': '10', 'IBM': '20', 'BAC': '20', 'GS': '10'}
        result = cons.get_position_sizes(scores, 0.1, 'Sector', 2)
        benchmark = {'AAPL': 0.25, 'GS': -0.25, 'IBM': 0.25, 'BAC': -0.25}
        self.assertDictEqual(result, benchmark)
        scores = {'AAPL': 4, 'IBM': 10, 'BAC': 4, 'GS': np.nan}
        result = cons.get_position_sizes(scores, 0.1, 'MarketCap', 1)

    def test_weight_group(self):
        data = pd.DataFrame({'score': [4, 2, 3]}, index=['AAPL', 'GS', 'IBM'])
        result = weight_group(data, 1, 10)
        benchmark = data.copy()
        benchmark = pd.DataFrame({'score': [2, 3, 4]},
                                 index=['GS', 'IBM', 'AAPL'])
        benchmark['weights'] = [-5, 0, 5.]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
