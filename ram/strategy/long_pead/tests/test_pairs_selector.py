import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.long_pead.data.pairs_selector import PairSelector
from ram.strategy.long_pead.data.pairs_selector import get_abs_distance


class TestPairSelector(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        pass

    def test_prep_output(self):
        data = pd.DataFrame({'A': [1, 2, 3, 4],
                             'B': [4, 2, 3, 1],
                             'C': [10, 12, 15, 19],
                             'D': [10, 8, 6, 7]})
        result = PairSelector()._prep_output(data)
        benchmark = pd.DataFrame()
        benchmark['Leg1'] = ['A', 'A', 'A', 'B', 'B', 'C']
        benchmark['Leg2'] = ['B', 'C', 'D', 'C', 'D', 'D']
        assert_frame_equal(result, benchmark)

    def test_get_abs_distance(self):
        data = np.array([[1, 2, 11], [3, 4, -17], [5, 6, 55], [7, 7, 17]])
        result = get_abs_distance(data[:, 0], data)
        benchmark = np.array([0, 3, 90])
        assert_array_equal(result, benchmark)
        result = get_abs_distance(data[:, 1], data)
        benchmark = np.array([3, 0, 89])
        assert_array_equal(result, benchmark)
        result = np.apply_along_axis(get_abs_distance, 0, data, data)
        benchmark = np.array([[0, 3, 90], [3, 0, 89], [90, 89, 0]])
        assert_array_equal(result, benchmark)

    def test_filter_pairs(self):
        data = pd.DataFrame([[1, 2, 11], [3, 4, -17], [5, 6, 55], [7, 7, 17]])
        data.columns = ['A', 'B', 'C']
        result = PairSelector()._filter_pairs(data)
        benchmark = pd.DataFrame()
        benchmark['Leg1'] = ['A', 'B', 'A']
        benchmark['Leg2'] = ['B', 'C', 'C']
        benchmark['distances'] = [6.5, 7.5, 10]
        assert_frame_equal(result, benchmark)

    def test_get_spreads_zscores(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 5 + ['B'] * 5 + ['C'] * 5
        data['Date'] = [dt.date(2010, 1, i) for i in range(1, 6)] * 3
        data['AdjClose'] = [1, 3, 5, 7, 9, 2, 4, 6, 7, 8, 11, 17, 55, 17, 16]
        flags = [False] * 4 + [True]
        data['TestFlag'] = flags * 3
        result = PairSelector().rank_pairs(data, z_window=3)

    def test_get_zscores(self):
        data = pd.DataFrame()
        data['V1'] = [10.0, 10.1, 9.87, 9.56]
        data['V2'] = [100.0, 102.3, 102.9, 104.0]
        result = PairSelector._get_zscores(data, 3)
        self.assertEqual(result.shape[0], 4)
        self.assertEqual(result.shape[1], 2)
        self.assertAlmostEqual(result.iloc[-1, 0], -1.045565)
        self.assertAlmostEqual(result.iloc[-1, 1], 1.08254254)

    def test_get_spread_index(self):
        pair_info = pd.DataFrame()
        pair_info['Leg1'] = ['A', 'A']
        pair_info['Leg2'] = ['B', 'C']
        data = pd.DataFrame()
        data['A'] = np.exp([10, 12, 11])
        data['B'] = np.exp([12, 11, 7])
        data['C'] = np.exp([8, 8, 9])
        result = PairSelector._get_spread_index(pair_info, data)
        benchmark = pd.DataFrame()
        benchmark['A~B'] = [-2, 1, 4.]
        benchmark['A~C'] = [2, 4, 2.]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
