import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array

from ram.strategy.long_pead.constructor.utils import *


class TestUtils(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-04-04',
                 '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08']
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 8
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['MA5_AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0, 0, 0]
        data['TestFlag'] = [True] * 8
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        self.data = data.append(data2).reset_index(drop=True)

    def test_ern_date_blackout(self):
        result = ern_date_blackout(self.data, -1, 1)
        benchmark = np.array([0, 1, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 1)
        benchmark = np.array([0, 0, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 3)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 0, 0]*2)
        assert_array_equal(result.blackout.values, benchmark)

    def test_ern_price_anchor(self):
        data = self.data.copy()
        data = ern_date_blackout(data, -1, 1)
        result = ern_price_anchor(data, 1, 3)
        benchmark = np.array([np.nan] * 4 + [4, 4, 5, 6.])
        benchmark = np.append(benchmark, benchmark * 10)
        assert_array_equal(result.anchor_price.values, benchmark)
        #
        result = ern_price_anchor(data, 0, 3)
        benchmark = np.array([np.nan] * 4 + [3, 4, 5, 6.])
        benchmark = np.append(benchmark, benchmark * 10)
        assert_array_equal(result.anchor_price.values, benchmark)
        #
        data2 = self.data.copy()
        data2 = ern_date_blackout(data2, -1, 1)
        data2['SecCode'] = ['1234'] * 16
        result = ern_price_anchor(data2, 1, 3)
        benchmark = np.array([np.nan] * 4 + [4, 4, 5, 6, 7.] +
            [np.nan] * 3 + [40, 40, 50, 60.])
        assert_array_equal(result.anchor_price.values, benchmark)

    def test_ern_date_label(self):
        result = ern_date_label(self.data)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 1, 1.] * 2)
        assert_array_equal(result.ern_num.values, benchmark)
        #
        data2 = self.data.copy()
        data2['SecCode'] = ['1234'] * 16
        result = ern_date_label(data2)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2])
        assert_array_equal(result.ern_num.values, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
