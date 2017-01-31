import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.birds.genetic_search2 import GeneticSearch


class TestGeneticSearchClass(unittest.TestCase):

    def setUp(self):
        df = pd.DataFrame()
        df['SecCode'] = [1, 1, 1, 2, 2, 2]
        df['Date'] = [dt.datetime(2012, 1, 1), 
                      dt.datetime(2012, 1, 2), 
                      dt.datetime(2012, 1, 3)] * 2
        df['T_1'] = [dt.datetime(2012, 1, 2), 
                     dt.datetime(2012, 1, 3), 
                     dt.datetime(2012, 1, 4)] * 2
        df['T_2'] = [dt.datetime(2012, 1, 3), 
                     dt.datetime(2012, 1, 4), 
                     dt.datetime(2012, 1, 5)] * 2
        df['T_3'] = [dt.datetime(2012, 1, 4), 
                     dt.datetime(2012, 1, 5), 
                     dt.datetime(2012, 1, 6)] * 2
        df['ReturnDay1'] = [1, 2, 3] * 2
        df['ReturnDay2'] = [2, 3, 4] * 2
        df['ReturnDay3'] = [3, 4, 5] * 2
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

    def Xtest_init(self):
        gs = GeneticSearch(self.df, self.ests, self.ests)
        assert_array_equal(gs.daily_returns, [1]*2+[2]*4+[3]*6+[4]*4+[5]*2)
        assert_array_equal(gs.reshaped_row_number[:2],
                           [[0, 6, 12], [1, 7, 13]])
        benchmark = np.array(
            [[[2, 1], [1, 2], [1, 2]], [[1, 2], [1, 2], [1, 2]]])
        assert_array_equal(gs.ests_array_long, benchmark)

    def Xtest_bucket_mean(self):
        x = np.array([1, 1, 2, 2, 2, 3, 6, 8, 8])
        y = np.array([1, 3, 2, 3, 4, 9, 10, 12, 14])
        result = GeneticSearch._bucket_mean(x, y)
        assert_array_equal(result[0], [1, 2, 3, 6, 8])
        assert_array_equal(result[1], [2, 3, 1, 1, 2])
        assert_array_equal(result[2], [2, 3, 9, 10, 13])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
