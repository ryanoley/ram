import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.birds.pairs import *

from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.util.testing import assert_frame_equal


class TestPairs(unittest.TestCase):

    def setUp(self):
        test_dates = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]]
        data = pd.DataFrame(index=test_dates)
        data['V2'] = [0.02, -0.01, 0.40, -0.2, 0.3]
        data['V1'] = [0.04, -0.02, 0.30, -0.1, -0.01]
        data['V3'] = [-0.04, 0.02, -0.30, 0.1, 0.01]
        self.data_frame = data
        data = np.array(data)
        index = np.cumsum(data, axis=0)
        self.data = data
        self.index = index

    def test_get_best_pairs(self):
        result = Pairs().get_best_pairs(self.data_frame,
                                        cut_date=dt.date(2010, 1, 4),
                                        z_window=2,
                                        max_pairs=2)
        benchmark = pd.DataFrame(index=[dt.date(2010, 1, 4),
                                        dt.date(2010, 1, 5)])
        benchmark['V2_V1'] = [-0.10, 0.31]
        benchmark['V2_V3'] = [-0.30, 0.29]
        assert_frame_equal(result[0], benchmark)
        benchmark['V2_V1'] = [-0.707107, 0.707107]
        benchmark['V2_V3'] = [-0.707107, 0.707107]
        assert_frame_equal(result[1], benchmark)
        benchmark = pd.DataFrame()
        benchmark['Leg1'] = ['V2', 'V2']
        benchmark['Leg2'] = ['V1', 'V3']
        benchmark['score'] = [0, 3]
        assert_frame_equal(result[2][['Leg1', 'Leg2', 'score']], benchmark)

    def test_create_output_object(self):
        result = Pairs()._create_output_object(self.data_frame)
        benchmark = pd.DataFrame()
        benchmark['Leg1'] = ['V2', 'V2', 'V1']
        benchmark['Leg2'] = ['V1', 'V3', 'V3']
        assert_frame_equal(result, benchmark)

    def test_get_stats_all_pairs_funcs(self):
        # _get_corr_coef
        result = Pairs()._get_corr_coef(self.data)
        self.assertTupleEqual(result.shape, (3, 3))
        # _get_corr_moves
        result = Pairs()._get_corr_moves(self.data[:, 0], self.data)
        benchmark = np.array([1, 0.8, 0.2])
        assert_array_equal(result, benchmark)
        result = Pairs()._get_corr_moves(self.data[:, 1], self.data)
        benchmark = np.array([0.8, 1, 0.0])
        assert_array_equal(result, benchmark)
        result = Pairs()._get_corr_moves(self.data[:, 2], self.data)
        benchmark = np.array([0.2, 0, 1])
        assert_array_equal(result, benchmark)
        result = np.apply_along_axis(Pairs()._get_corr_moves, 0,
                                     self.data, self.data)
        benchmark = np.array([[1, 0.8, 0.2], [0.8, 1, 0], [0.2, 0, 1]])
        assert_array_equal(result, benchmark)
        # _get_vol_ratios
        result = Pairs()._get_vol_ratios(self.data[:, 0], self.data)
        benchmark = np.array([1, 0.62541499, 0.62541499])
        assert_array_almost_equal(result, benchmark)
        result = np.apply_along_axis(Pairs()._get_vol_ratios, 0,
                                     self.data, self.data)
        benchmark = np.array([[1, 1.59893833, 1.59893833],
                              [0.62541499, 1, 1],
                              [0.62541499, 1, 1]])
        assert_array_almost_equal(result, benchmark)
        # _get_abs_distance
        result = Pairs()._get_abs_distance(self.index[:, 0], self.index)
        benchmark = np.array([0., 0.43, 1.97])
        assert_array_almost_equal(result, benchmark)
        result = np.apply_along_axis(Pairs()._get_abs_distance, 0,
                                     self.index, self.index)
        benchmark = np.array([[0, 0.43, 1.97],
                              [0.43, 0, 1.62],
                              [1.97, 1.62, 0]])
        assert_array_almost_equal(result, benchmark)

    def test_extract_data_from_2d_array(self):
        z1 = list(it.combinations(range(self.data.shape[1]), 2))
        X1 = np.apply_along_axis(Pairs()._get_vol_ratios, 0,
                                 self.index, self.index)
        result = Pairs()._create_output_object(self.data_frame)
        result['volratio'] = [X1[z1[i]] for i in range(len(z1))]
        benchmark = pd.DataFrame()
        benchmark['Leg1'] = ['V2', 'V2', 'V1']
        benchmark['Leg2'] = ['V1', 'V3', 'V3']
        benchmark['volratio'] = [1.760993, 1.760993, 1]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
