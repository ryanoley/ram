import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.momentum.constructor import MomentumConstructor


class TestMomentumConstructor(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1), dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3), dt.datetime(2015, 1, 4)]
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 4
        data['Date'] = dates
        data['AdjClose'] = [1, 2, 3, 4]
        data['MA5_AdjClose'] = [1, 2, 3, 4]
        data['MA80_AdjClose'] = [1, 2, 3, 4]
        data['GSECTOR'] = [10] * 4
        data['EARNINGSFLAG'] = [0] * 4
        data['TestFlag'] = [False] * 4
        self.data = data
        self.dates = dates

    def test_format_data(self):
        import pdb; pdb.set_trace()
        con = MomentumConstructor()
        cons._format_data(self.data, self.dates)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
