import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.statarb.version_001.constructor.constructor_pairs import *

from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _select_port_and_offsets
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _merge_scores_zscores_data
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _format_scores_dict
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _extract_zscore_data
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _get_weighting
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _zscore_rank


class TestConstructorPairs(unittest.TestCase):

    def setUp(self):
        zscores = pd.DataFrame(index=[dt.date(2010, 1, 1), dt.date(2010, 1, 2)])
        zscores['A~B'] = [10, 12]
        zscores['A~C'] = [-10, -12]
        zscores['B~C'] = [2, 4]
        zscores_pair_info = pd.DataFrame()
        zscores_pair_info['pair'] = ['A~B', 'A~C', 'B~C']
        zscores_pair_info['Leg1'] = ['A', 'A', 'B']
        zscores_pair_info['Leg2'] = ['B', 'C', 'C']
        zscores_pair_info['distances'] = [1, 2, 3]
        self.data = {}
        self.data['zscores'] = zscores
        self.data['pair_info'] = zscores_pair_info

    def Xtest_select_port_and_offsets(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A', 'A', 'A', 'A', 'A', 'B']
        data['OffsetSecCode'] = ['B', 'C', 'D', 'E', 'F', 'G']
        data['Signal'] = [2, 2, 2, 2, 2, -1]
        data['SignalOffset'] = [-1, 1, -1, 1, -1, 1]
        data['distances'] = [1, 2, 3, 4, 5, 6]
        data['zscore'] = [-1.5, -1.5, -3, 2, 0, 10]

    def test_extract_zscore_data(self):
        result = _extract_zscore_data(self.data, dt.date(2010, 1, 2))
        benchmark = pd.DataFrame()
        benchmark['pair'] = ['A~B', 'A~C', 'B~C']
        benchmark['zscore'] = [12, -12, 4]
        benchmark['Leg1'] = ['A', 'A', 'B']
        benchmark['Leg2'] = ['B', 'C', 'C']
        benchmark['distances'] = [1, 2, 3]
        assert_frame_equal(result, benchmark)

    def test_format_scores_dict(self):
        scores = {'A': 1.2, 'B': 2.2, 'C': -1.2, 'D': np.nan, 'E': np.nan}
        result = _format_scores_dict(scores)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['C', 'A', 'B']
        benchmark['RegScore'] = [-1.2, 1.2, 2.2]
        assert_frame_equal(result, benchmark)

    def test_merge_scores_zscores_data(self):
        scores = pd.DataFrame()
        scores['SecCode'] = ['C', 'A', 'B', 'D', 'E']
        scores['RegScore'] = [-1, 0, 2, np.nan, np.nan]
        zscores = pd.DataFrame()
        zscores['zscore'] = [10, 20, 30]
        zscores['Leg1'] = ['A', 'A', 'B']
        zscores['Leg2'] = ['B', 'C', 'C']
        zscores['distances'] = [1, 2, 3]
        result = _merge_scores_zscores_data(scores, zscores)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['A', 'A', 'B']
        benchmark['OffsetSecCode'] = ['B', 'C', 'C']
        benchmark['Signal'] = [0, 0, 2.0]
        benchmark['SignalOffset'] = [2, -1, -1.0]
        benchmark['distances'] = [1, 2, 3]
        benchmark['zscore'] = [10, 20, 30]
        assert_frame_equal(result, benchmark)

    def test_get_weighting(self):
        data = pd.DataFrame({'V1': [3, 1, 2]})
        result = _get_weighting(data, 'V1', 1)
        benchmark = pd.DataFrame()
        benchmark['V1'] = [1, 2, 3]
        benchmark['Weighted_V1'] = [-0.5, 0, 0.5]
        assert_frame_equal(result, benchmark)

    def test_zscore_rank(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A', 'A', 'A', 'A', 'B', 'B']
        data['OffsetSecCode'] = ['B', 'C', 'D', 'E', 'F', 'G']
        data['zscore'] = [2, 3, 1, 0, -1, -5]
        data['Signal'] = range(6)
        result = _zscore_rank(data)
        result['benchmark'] = [1, 2, 3, 4, 1, 2.]
        assert np.all(result.benchmark == result.zscore_rank)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
