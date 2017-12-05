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
    _format_scores_dict
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _get_weighting
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _zscore_rank
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _merge_zscores_signals
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _merge_zscores_pair_info


class TestConstructorPairs(unittest.TestCase):

    def setUp(self):
        zscores = pd.DataFrame(index=[dt.date(2010, 1, 1),
                                      dt.date(2010, 1, 2)])
        zscores['A~B'] = [1, 2]
        zscores['A~C'] = [-4, -5]
        zscores['B~A'] = [-1, -2]
        zscores['B~C'] = [9, 8]
        zscores['C~A'] = [4, 5]
        zscores['C~B'] = [-9, -8]
        #
        pair_info = pd.DataFrame()
        pair_info['PrimarySecCode'] = ['A', 'A', 'B', 'B', 'C', 'C']
        pair_info['OffsetSecCode'] = ['B', 'C', 'A', 'C', 'A', 'B']
        pair_info['distances'] = [1, 2, 3, 4, 5, 6]
        pair_info['distance_rank'] = [1, 1, 1, 1, 1, 1]
        pair_info['pair'] = ['A~B', 'A~C', 'B~A', 'B~C', 'C~A', 'C~B']
        #
        signals = pd.DataFrame()
        signals['SecCode'] = ['A'] * 2 + ['B'] * 2 + ['C'] * 2
        signals['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 3
        signals['preds'] = range(6)
        self.signals = signals
        self.container = {}
        self.container['zscores'] = zscores
        self.container['pair_info'] = pair_info

    def Xtest_set_signals_constructor_data(self):
        cons = PortfolioConstructorPairs()
        cons.set_signals_constructor_data(self.signals, self.container)
        result = cons._zscores.reset_index()
        benchmark = pd.DataFrame()
        pairs = self.pair_info.pair.tolist() * 2
        pairs.sort()
        benchmark['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 6
        benchmark['pair'] = pairs
        benchmark['SecCode'] = ['A'] * 4 + ['B'] * 4 + ['C'] * 4
        benchmark['OffsetSecCode'] = ['B', 'B', 'C', 'C', 'A', 'A',
                                      'C', 'C', 'A', 'A', 'B', 'B']
        benchmark['distances'] = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]
        benchmark['distance_rank'] = 1
        benchmark['zscore'] = [1, 2, -4, -5, -1, -2, 9, 8, 4, 5, -9, -8]
        benchmark['Signal'] = [0, 1, 0, 1, 2, 3, 2, 3, 4, 5, 4, 5]
        benchmark['OffsetSignal'] = [2, 3, 4, 5, 0, 1, 4, 5, 0, 1, 2, 3]
        assert_frame_equal(result, benchmark)

    def test_select_port_and_offsets(self):
        cons = PortfolioConstructorPairs()
        cons.set_signals_constructor_data(self.signals, self.container)
        params = {'type': 'basic'}
        zscores = cons._zscores.loc[dt.date(2010, 1, 1)]
        scores = {'A': 0, 'C': 4, 'B': 2}
        result = _select_port_and_offsets(scores, zscores, params)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['A', 'B', 'C']
        benchmark['Signal'] = [0, 2, 4]
        benchmark['pos_size'] = [-0.5, 0, 0.5]
        assert_frame_equal(result, benchmark)
        #
        # 30: Unrealistic param but need two observations
        params = {'type': 'tree_model_long', 'pair_offsets': 1,
                  'signal_thresh_perc': 30}
        zscores = cons._zscores.loc[dt.date(2010, 1, 1)]

        result = _select_port_and_offsets(scores, zscores, params)
        #
        params = {'type': 'tree_model', 'pair_offsets': 1}
        zscores = cons._zscores.loc[dt.date(2010, 1, 1)]
        result = _select_port_and_offsets(scores, zscores, params)

    def test_format_scores_dict(self):
        scores = {'A': 1.2, 'B': 2.2, 'C': -1.2, 'D': np.nan, 'E': np.nan}
        result = _format_scores_dict(scores)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['C', 'A', 'B']
        benchmark['RegScore'] = [-1.2, 1.2, 2.2]
        assert_frame_equal(result, benchmark)

    def test_get_weighting(self):
        data = pd.DataFrame({'V1': [3, 1, 2, np.nan, 4, np.nan]})
        result = _get_weighting(data.V1, False, 1)
        self.assertAlmostEqual(np.sum(result), 0)
        self.assertAlmostEqual(np.sum(np.abs(result)), 1)
        data = pd.DataFrame({'V1': [3, 2, np.nan, np.nan, 1, np.nan]})
        result = _get_weighting(data.V1, False, 1)
        benchmark = np.array([0.5, 0, 0, 0, -0.5, 0])
        assert_array_equal(result, benchmark)
        # Repeated values
        x1 = np.array([0, 1, 2, 3])
        x2 = np.array([0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3])
        result1 = _get_weighting(x1, True, 1)
        result2 = _get_weighting(x2, True, 1)
        self.assertAlmostEqual(result1[0], result2[:3].sum())
        self.assertAlmostEqual(result1[1], result2[3:6].sum())
        self.assertAlmostEqual(result1[2], result2[6:9].sum())
        self.assertAlmostEqual(result1[3], result2[9:12].sum())

    def test_zscore_rank(self):
        data = pd.DataFrame()
        data['pair'] = ['A~B', 'A~C', 'B~A', 'B~C'] * 2
        data['Date'] = ['2010-01-01'] * 4 + ['2010-01-02'] * 4
        data['SecCode'] = ['A', 'A', 'B', 'B'] * 2
        data['OffsetSecCode'] = ['B', 'C', 'A', 'C'] * 2
        data['zscore'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data = data.set_index(['pair', 'Date'])
        result = _zscore_rank(data)
        benchmark = [1, 2, 1, 2, 1, 2, 1, 2.]
        self.assertEqual(result[0].tolist(), benchmark)
        benchmark = [2.] * 8
        self.assertEqual(result[1].tolist(), benchmark)

    def test_merge_zscores_pair_info(self):
        _merge_zscores_pair_info

    def test_merge_zscores_signals(self):
        _merge_zscores_signals

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
