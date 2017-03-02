import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

import ram.strategy.statarb.pairselector.pairs3 as pairs3


class TestPairsStrategy3(unittest.TestCase):

    def setUp(self):
        # Create fpairs frame
        scored_pairs = pd.DataFrame()
        scored_pairs['Leg1'] = ['a', 'a', 'b', 'x']
        scored_pairs['Leg2'] = ['b', 'c', 'd', 'a']
        scored_pairs['score'] = [1, 2, 3, 10]
        self.scored_pairs = scored_pairs
        # Create data frame with missing data points
        data = pd.DataFrame()
        data['SecCode'] = ['a', 'a', 'a', 'b', 'b', 'b',
                           'c', 'c', 'c', 'd', 'd', 'd']
        data['Date'] = [1, 2, 3] * 4
        data['AdjClose'] = [100, 101, 102, 100, 99, 98,
                            100, 104, 106, 100, 97, 100]
        data['Var1'] = range(1, 13)
        data['Var2'] = range(2, 14)
        self.data = data.drop([8]).reset_index(drop=True)
        self.features = ['Var1', 'Var2']

    def test_clean_scored_pairs_df(self):
        result = pairs3._clean_scored_pairs_df(self.scored_pairs, self.data)
        benchmark = pd.DataFrame([])
        benchmark['Leg1'] = ['a', 'a', 'b']
        benchmark['Leg2'] = ['b', 'c', 'd']
        benchmark['score'] = [1, 2, 3]
        assert_frame_equal(result, benchmark)

    def test_make_return_column(self):
        result = pairs3._make_return_column(self.data)
        benchmark = self.data.copy()
        benchmark['Ret'] = [0.02, np.nan, np.nan, -0.02, np.nan,
                            np.nan, np.nan, np.nan, 0, np.nan, np.nan]
        assert_frame_equal(result, benchmark)

    def test_match_pair_feature_data(self):
        scored_pairs = pairs3._clean_scored_pairs_df(self.scored_pairs,
                                                     self.data)
        data = pairs3._make_return_column(self.data)
        data['Ret'] = range(1, 12)
        result = pairs3._match_pair_feature_data(data, scored_pairs,
                                                 self.features)
        benchmark = pd.DataFrame([])
        benchmark['Date'] = [1, 2, 3, 1, 2, 1, 2, 3]
        benchmark['Pair'] = ['a~b', 'a~b', 'a~b', 'a~c',
                             'a~c', 'b~d', 'b~d', 'b~d']
        benchmark['Ret_x'] = [1, 2, 3, 1, 2, 4, 5, 6]
        benchmark['Ret_y'] = [4, 5, 6, 7, 8, 9, 10, 11]
        benchmark['Var1_x'] = [1, 2, 3, 1, 2, 4, 5, 6]
        benchmark['Var2_x'] = [2, 3, 4, 2, 3, 5, 6, 7]
        benchmark['Var1_y'] = [4, 5, 6, 7, 8, 10, 11, 12]
        benchmark['Var2_y'] = [5, 6, 7, 8, 9, 11, 12, 13]
        assert_frame_equal(result, benchmark)

    def test_make_responses(self):
        scored_pairs = pairs3._clean_scored_pairs_df(self.scored_pairs,
                                                     self.data)
        data = pairs3._make_return_column(self.data)
        data['Ret'] = range(1, 12)
        feature_data = pairs3._match_pair_feature_data(
            data, scored_pairs, self.features)
        result = pairs3._make_responses(feature_data)
        feature_data = feature_data.sort_values(
            ['Date', 'Pair']).reset_index(drop=True)
        feature_data = feature_data.drop(['Ret_x', 'Ret_y'], axis=1)
        feature_data['Response1'] = [True, False, False, True, False,
                                     False, True, False]
        feature_data['Response2'] = [False, True, False, False, True,
                                     False, False, True]
        assert_frame_equal(result, feature_data)

    def test_get_zscores(self):
        close_prices = self.data.pivot(index='Date',
                                       columns='SecCode',
                                       values='AdjClose')
        scored_pairs = self.scored_pairs.iloc[:-1]
        result = pairs3._get_zscores(close_prices, scored_pairs, [2, 3])
        benchmark = pd.DataFrame([])
        benchmark['Pair'] = ['a~b', 'a~b', 'a~b', 'a~c', 'a~c',
                             'a~c', 'b~d', 'b~d', 'b~d']
        benchmark['Date'] = [1, 2, 3, 1, 2, 3, 1, 2, 3]
        benchmark['ZScore2'] = [np.nan, 0.707107, 0.707107, np.nan, -0.707107,
                                np.nan, np.nan, 0.707107, -0.707107]
        benchmark['ZScore3'] = [np.nan, np.nan, 1.00003, np.nan, np.nan,
                                np.nan, np.nan, np.nan, -0.998304]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
