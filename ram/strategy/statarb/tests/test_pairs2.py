import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.pairselector.pairs2 import PairsStrategy2


class TestPairsStrategy2(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame()
        data['SecCode'] = ['V1'] * 10 + ['V2'] * 10 + ['V3'] * 10 + ['V4'] * 10
        data['AdjClose'] = \
            [100, 101, 102, 100.3, 101, 105, 109, 110, 104, 101.] + \
            [100, 99, 98, 103, 97, 95, 92, 87, 94, 99.] + \
            [50, 52, 55, 65, 60, 59, 58, 57, 56, 59.] + \
            [23, 25, 22, 20, 29, 30, 31, 32, 33, 28.]
        data['GSECTOR'] = [20] * 40
        data['Date'] = pd.DatetimeIndex(
            [dt.datetime(2015, 1, i) for i in range(1, 11)] * 4)
        self.data = data

    def test_get_best_pairs(self):
        pairs = PairsStrategy2()
        cut_date = dt.datetime(2015, 1, 9)
        result = pairs.get_best_pairs(self.data, cut_date,
                                      n_per_side=2, max_pairs=10, z_window=3)

    def test_get_clean_ids(self):
        close_data = self.data.pivot(index='Date',
                                     columns='SecCode',
                                     values='AdjClose')
        close_data.iloc[2, 2] = np.nan
        cut_date = dt.datetime(2015, 1, 8)
        result = PairsStrategy2._get_clean_ids(close_data,
                                               cut_date=cut_date)
        benchmark = ['V1', 'V2', 'V4']
        self.assertListEqual(result, benchmark)

    def test_classify_sectors(self):
        result = PairsStrategy2._classify_sectors(self.data)
        benchmark = {20: ['V1', 'V2', 'V3', 'V4']}
        self.assertDictEqual(result, benchmark)

    def test_concatenate_seccodes(self):
        seccodes = ['V1', 'V2', 'V3', 'V4']
        combs1 = np.array([[0, 1], [0, 2], [0, 3], [0, 0]])
        combs2 = np.array([[2, 3], [1, 3], [1, 2], [3, 1]])
        result = PairsStrategy2._concatenate_seccodes(seccodes, combs1, combs2)
        benchmark = np.array(['V1_V2~V3_V4', 'V1_V3~V2_V4',
                              'V1_V4~V2_V3', 'V1_V1~V4_V2'])
        assert_array_equal(result, benchmark)

    def test_filter_combs(self):
        combs = np.array([
            [1, 1, 89, 93],
            [1, 2, 3, 6],
            [3, 6, 1, 2],
            [1, 2, 7, 9],
            [3, 5, 1, 2]
        ])
        c1, c2 = combs[:, :2], combs[:, 2:]
        result = PairsStrategy2._filter_combs(c1, c2)
        benchmark = np.array([[1, 2], [1, 2], [1, 2]])
        assert_array_equal(result[0], benchmark)
        benchmark = np.array([[3, 5], [3, 6], [7, 9]])
        assert_array_equal(result[1], benchmark)
        #
        combs = np.array([
            [0, 1, 2, 3],
            [0, 3, 1, 2],
            [0, 2, 1, 3]
        ])
        c1, c2 = combs[:, :2], combs[:, 2:]
        result = PairsStrategy2._filter_combs(c1, c2)
        benchmark = np.array([[0, 1], [0, 2], [0, 3]])
        assert_array_equal(result[0], benchmark)
        benchmark = np.array([[2, 3], [1, 3], [1, 2]])
        assert_array_equal(result[1], benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
