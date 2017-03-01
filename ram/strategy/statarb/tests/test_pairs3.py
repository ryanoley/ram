import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.pairselector.pairs3 import *


class TestPairsStrategy3(unittest.TestCase):

    def setUp(self):
        pass

    def test_merge_pairs(self):
        # Create fpairs frame
        fpairs = pd.DataFrame()
        fpairs['Leg1'] = ['a', 'a', 'b']
        fpairs['Leg2'] = ['b', 'c', 'd']
        fpairs['score'] = [1, 2, 3]
        # Create data frame
        df = pd.DataFrame()
        df['SecCode'] = ['a', 'a', 'a', 'b', 'b', 'b',
                         'c', 'c', 'c', 'd', 'd', 'd']
        df['Date'] = [1, 2, 3] * 4
        df['AdjClose'] = [100, 101, 102, 100, 99, 98,
                          100, 104, 106, 100, 97, 100]
        df['Var1'] = range(1, 13)
        df['Var2'] = range(2, 14)
        # Introduce missing datapoints
        df = df.drop([8]).reset_index(drop=True)
        features = ['Var1', 'Var2']
        # Test1
        import pdb; pdb.set_trace()
        result = merge_pairs(df, fpairs, features)
        benchmark = pd.DataFrame()
        benchmark['Date'] = [1, 2, 3, 1, 2, 1, 2, 3]
        benchmark['Var1_x'] = [1, 2, 3, 1, 2, 4, 5, 6]
        benchmark['Var1_y'] = [4, 5, 6, 7, 8, 10, 11, 12]
        benchmark['Var2_x'] = [2, 3, 4, 2, 3, 5, 6, 7]
        benchmark['Var2_y'] = [5, 6, 7, 8, 9, 11, 12, 13]
        assert_frame_equal(result, benchmark)
        # Create fpairs frame
        fpairs = pd.DataFrame()
        fpairs['Leg1'] = ['b', 'b', 'd']
        fpairs['Leg2'] = ['a', 'c', 'a']
        fpairs['score'] = [1, 2, 3]
        # Test2
        result = merge_pairs(df, fpairs, features)
        benchmark = pd.DataFrame()
        benchmark['Date'] = [1, 2, 3, 1, 2, 1, 2, 3]
        benchmark['Var1_x'] = [4, 5, 6, 4, 5, 10, 11, 12]
        benchmark['Var1_y'] = [1, 2, 3, 7, 8, 1, 2, 3]
        benchmark['Var2_x'] = [5, 6, 7, 5, 6, 11, 12, 13]
        benchmark['Var2_y'] = [2, 3, 4, 8, 9, 2, 3, 4]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
