import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.pairselector.pairs2 import *


class TestPairsSelector2(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_get_return_series(self):
        enter_z = 2
        exit_z = 0
        zscores = np.array([0, 2, 3, 2, 1, -1, 0, 1, 0.])
        close1 = np.array([100, 104, 103, 102, 105, 100, 99, 96, 95.])
        close2 = np.array([50, 52, 54, 53, 51, 47, 45, 44, 43.])
        returns = np.zeros(len(close1))
        count = get_return_series(enter_z, exit_z,
                                  zscores, close1,
                                  close2, returns)
        benchmark = np.array([0, 0, 0.04807692, -0.00880978, -0.06714761,
                              -0.03081232, 0, 0, 0])
        assert_array_equal(returns.round(5), benchmark.round(5))
        self.assertEqual(count, 1)
        zscores = np.array([0, -2, -3, -2, -1, -1, -1, 0, 0])
        returns = np.zeros(len(close1))
        count = get_return_series(enter_z, exit_z,
                                  zscores, close1,
                                  close2, returns)
        benchmark = np.array([0, 0, -0.04807692, 0.00880978, 0.06714761,
                              0.03081232, 0.03255319, -0.00808081, 0])
        assert_array_equal(returns.round(5), benchmark.round(5))
        self.assertEqual(count, 1)

    def test_get_trade_signal_series(self):
        zscores = np.array([[0, -3], [2, -2], [3, 1], [0, 3]])
        close1 = np.array([[100, 100], [100, 100], [95, 95], [90, 85.]])
        close2 = np.array([[100, 100], [100, 104], [105, 110], [200, 200.]])
        result = get_trade_signal_series(zscores, close1, close2,
                                         enter_z=2, exit_z=0)
        benchmark = np.array([[0, -.15], [1.1, 0], [0, 0], [0, 0.]])
        assert_array_equal(result.round(4), benchmark.round(4))

    def test_get_abs_distance(self):
        x = np.array([1, 1, 1.])
        y = np.array([[2, 3], [2, 3], [2, 3.]])
        result = get_abs_distance(x, y)
        benchmark = np.array([3, 6.])
        assert_array_equal(result, benchmark)

    def test_get_distances(self):
        close_data = pd.DataFrame({
            'V1': [1, 1, 1], 'V2': [2, 4, 8], 'V3': [3, 9, 27.]})
        result = PairSelector2._get_distances(close_data)
        benchmark = np.array([[0, 4, 10], [4, 0, 6], [10, 6, 0.]])
        assert_array_equal(result, benchmark)

    def test_prep_output(self):
        close_data = pd.DataFrame({
            'V1': [1, 1, 1], 'V2': [2, 4, 8], 'V3': [3, 9, 27.]})
        result = PairSelector2._prep_output(close_data)
        benchmark = pd.DataFrame({'Leg1': ['V1', 'V1', 'V2']})
        benchmark['Leg2'] = ['V2', 'V3', 'V3']
        assert_frame_equal(result, benchmark)

    def test_flatten(self):
        data = np.array([[0, 4, 10], [4, 0, 6], [10, 6, 0.]])
        result = PairSelector2._flatten(data)
        benchmark = [4.0, 10.0, 6.0]
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
