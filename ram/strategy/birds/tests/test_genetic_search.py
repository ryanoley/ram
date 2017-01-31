import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.birds import genetic_search as gs


class TestGeneticSearch(unittest.TestCase):

    def setUp(self):
        df = pd.DataFrame()
        df['SecCode'] = [1, 1, 1, 2, 2, 2]
        df['Date'] = [1, 2, 3] * 2
        df['T_1'] = df.Date + 1
        df['T_2'] = df.Date + 2
        df['T_3'] = df.Date + 3
        df['ReturnDay1'] = range(6)
        df['ReturnDay2'] = range(6)
        df['ReturnDay3'] = range(6)
        # Estimates
        ests = pd.DataFrame()
        ests[0] = [2, 1, 1, 1, 2, 2]
        ests[1] = [1, 1, 1, 2, 2, 2]
        self.df = df
        self.ests = ests

        df = pd.DataFrame()
        df['SecCode'] = [3, 1, 1, 2, 2, 2]
        df['Date'] = [1, 2, 3] * 2
        df['T_1'] = df.Date + 1
        df['T_2'] = df.Date + 2
        df['T_3'] = df.Date + 3
        df['ReturnDay1'] = range(6)
        df['ReturnDay2'] = range(6)
        df['ReturnDay3'] = range(6)
        # Estimates
        ests = pd.DataFrame()
        ests[0] = [2, 1, 1, 1, 2, 2]
        self.df2 = df
        self.ests2 = ests

        df = pd.DataFrame()
        df['SecCode'] = [1, 1, 2, 2, 3, 3]
        df['Date'] = [1, 2] * 3
        df['T_1'] = df.Date + 1
        df['T_2'] = df.Date + 2
        df['T_3'] = df.Date + 3
        df['ReturnDay1'] = range(6)
        df['ReturnDay2'] = range(6)
        df['ReturnDay3'] = range(6)
        # Estimates
        ests = pd.DataFrame()
        ests[0] = [2, 1, 3, 2, 1, 3]
        ests[1] = [1, 2, 2, 1, 3, 3]
        self.df3 = df
        self.ests3 = ests

        np.random.seed(123)
        df = pd.DataFrame()
        df['SecCode'] = sorted([1, 2, 3, 4, 5] * 10)
        df['Date'] = range(1, 11) * 5
        df['T_1'] = df.Date + 1
        df['T_2'] = df.Date + 2
        df['T_3'] = df.Date + 3
        df['ReturnDay1'] = np.random.randn(50)
        df['ReturnDay2'] = np.random.randn(50)
        df['ReturnDay3'] = np.random.randn(50)
        self.df4 = df
        # Estimates
        ests = pd.DataFrame()
        ests[0] = np.random.rand(50)
        ests[1] = np.random.rand(50)
        ests[2] = np.random.rand(50)
        ests[3] = np.random.rand(50)
        ests[4] = np.random.rand(50)
        self.estsL4 = ests
        ests = pd.DataFrame()
        ests[0] = np.random.rand(50)
        ests[1] = np.random.rand(50)
        ests[2] = np.random.rand(50)
        ests[3] = np.random.rand(50)
        ests[4] = np.random.rand(50)
        self.estsS4 = ests

    def Xtest_get_optimal_combination(self):
        z = gs.get_optimal_combination(self.df4, self.estsL4, self.estsS4)

    def test_make_weighted_estimate(self):
        ests, rows = gs.make_estimate_arrays(self.df, self.ests)
        weights = np.array([10, 2])
        result = gs.make_weighted_estimate(ests, weights)
        benchmark = np.array([[11, 7], [6, 12], [6, 12.]])
        assert_array_equal(result, benchmark)

    def test_bucket_mean(self):
        bins = [1, 1, 2, 2, 2]
        vals = [1, 2, 3, 4, 5]
        x, y, z = gs._bucket_mean(bins, vals)
        assert_array_equal(x, [1, 2])
        assert_array_equal(y, [2, 3])
        assert_array_equal(z, [1.5, 4])

    def test_make_estimate_arrays(self):
        ests, rows = gs.make_estimate_arrays(self.df, self.ests)
        benchmark = np.array([[[2, 1], [1, 2], [1, 2]], [[1, 2], [1, 2], [1, 2]]])
        assert_array_equal(ests, benchmark)
        assert_array_equal(rows, np.array([[0, 3], [1, 4], [2, 5]]))

        ests, rows = gs.make_estimate_arrays(self.df2, self.ests2)
        result = np.array([[[np.nan, 1, 2], [1, 2, np.nan], [1, 2, np.nan]]])
        assert_array_equal(ests, result)
        assert_array_equal(rows, np.array([[np.nan, 3, 0],
            [1, 4, np.nan], [2, 5, np.nan]]))

        ests, rows = gs.make_estimate_arrays(self.df3, self.ests3)
        benchmark = np.array([[[2, 3, 1], [1, 2, 3]], [[1, 2, 3], [2, 1, 3]]])
        assert_array_equal(ests, benchmark)
        assert_array_equal(rows, np.array([[0, 2, 4], [1, 3, 5]]))

    def test_init_population(self):
        pop = gs.init_population(2, 5)
        self.assertAlmostEquals(pop[0][0].sum(), 1)
        self.assertAlmostEquals(pop[0][1].sum(), 1)
        self.assertIsInstance(pop[0], tuple)

    def test_get_min_est_rows(self):
        ests = np.array([[2, 1, 3], [1, 2, 3]])
        rows = np.array([[1, 3, 5], [2, 4, 6]])
        result = gs.get_min_est_rows(ests, rows, 2)
        benchmark = np.array([1, 2, 3, 4])
        assert_array_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
