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
        import pdb; pdb.set_trace()
        result = cons.get_position_sizes(scores, 0.1, 2)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
