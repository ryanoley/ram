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

    def test_get_optimal_combination(self):
        import pdb; pdb.set_trace()
        z = gs.get_optimal_combination(self.df, self.ests, self.ests)

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
        result = np.array([[[2, 1], [1, 2], [1, 2]], [[1, 2], [1, 2], [1, 2]]])
        assert_array_equal(ests, result)
        assert_array_equal(rows, np.array([[0, 3], [1, 4], [2, 5]]))
        ests, rows = gs.make_estimate_arrays(self.df2, self.ests2)
        result = np.array([[[np.nan, 1, 2], [1, 2, np.nan], [1, 2, np.nan]]])
        assert_array_equal(ests, result)
        assert_array_equal(rows, np.array([[np.nan, 3, 0],
            [1, 4, np.nan], [2, 5, np.nan]]))

    def test_init_population(self):
        pop = gs.init_population(2, 5)
        self.assertAlmostEquals(pop[0].sum(), 1)
        self.assertAlmostEquals(pop[1].sum(), 1)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
