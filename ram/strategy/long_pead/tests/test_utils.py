import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array

from ram.strategy.long_pead.utils import *


class TestUtils(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-04-04',
                 '2015-01-05', '2015-01-06']
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 6
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = [1, 2, 3, 4, 5, 6]
        data['MA5_AdjClose'] = [1, 2, 3, 4, 5, 6]
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0]
        data['TestFlag'] = [True] * 6
        data2 = data.copy()
        data2['SecCode'] =  ['5678'] * 6
        data2['AdjClose'] = data2.AdjClose * 10
        self.data = data.append(data2).reset_index(drop=True)

    def test_ern_date_blackout(self):
        result = ern_date_blackout(self.data, -1, 1)
        benchmark = np.array([0, 1, 1, 1, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 1)
        benchmark = np.array([0, 0, 1, 1, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 3)
        benchmark = np.array([0, 0, 1, 1, 1, 1]*2)
        assert_array_equal(result.blackout.values, benchmark)

    def test_ern_price_anchor(self):
        result = ern_price_anchor(self.data, 1)
        benchmark = np.array([0, 0, 0, 1, 0, 0]*2)
        assert_array_equal(result.anchor.values, benchmark)

    def test_anchor_returns(self):
        data = ern_date_blackout(self.data, -1, 1)
        data = ern_price_anchor(data, 1)
        import pdb; pdb.set_trace()
        result = anchor_returns(data)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
