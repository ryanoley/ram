import unittest
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.statarb.version_001.constructor.constructor_pairs import *
from ram.strategy.statarb.version_001.constructor.constructor_pairs import \
    _zscore_rank, _get_weighting, \
    _merge_zscores_signals, _merge_zscores_pair_info, \
    _tree_model_aggregate_pos_sizes, _basic, \
    _tree_model, _tree_model_get_pos_sizes, _final_format


class TestConstructorPairs(unittest.TestCase):

    def setUp(self):
        pass

    def test_set_signals_constructor_data(self):
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
        container = {}
        container['zscores'] = zscores
        container['pair_info'] = pair_info
        #
        cons = PortfolioConstructorPairs()
        cons.set_signals_constructor_data(signals, container)
        result = cons._zscores.reset_index()
        benchmark = pd.DataFrame()
        pairs = pair_info.pair.tolist() * 2
        pairs.sort()
        benchmark['Date'] = [pd.Timestamp('2010-01-01'),
                             pd.Timestamp('2010-01-02')] * 6
        benchmark['cindex'] = range(12)
        benchmark['pair'] = pairs
        benchmark['SecCode'] = ['A'] * 4 + ['B'] * 4 + ['C'] * 4
        benchmark['OffsetSecCode'] = ['B', 'B', 'C', 'C', 'A', 'A',
                                      'C', 'C', 'A', 'A', 'B', 'B']
        benchmark['distances'] = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]
        benchmark['distance_rank'] = 1
        benchmark['zscore'] = [1, 2, -4, -5, -1, -2, 9, 8, 4, 5, -9, -8]
        benchmark['Signal'] = [0, 1, 0, 1, 2, 3, 2, 3, 4, 5, 4, 5]
        benchmark['OffsetSignal'] = [2, 3, 4, 5, 0, 1, 4, 5, 0, 1, 2, 3]
        benchmark = benchmark.sort_values([
            'Date', 'SecCode', 'zscore']).reset_index(drop=True)
        benchmark['cindex'] = range(12)
        assert_frame_equal(result, benchmark)

    def test_tree_model(self):
        scores = {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50}
        cols = [x for x in itertools.permutations(['a', 'b', 'c',
                                                   'd', 'e'], 2)]
        data = pd.DataFrame()
        data['pair'] = ['{}~{}'.format(x, y) for x, y in cols]
        data['SecCode'] = [x[0] for x in cols]
        data['OffsetSecCode'] = [x[1] for x in cols]
        data['distances'] = range(len(cols))
        data['distance_rank'] = [1, 2, 3, 4] * 5
        # Assumed this column is sorted
        data['zscore'] = [-2, -1, 1, 2] * 5
        signals = pd.DataFrame()
        signals['SecCode'] = ['a', 'b', 'c', 'd', 'e']
        signals['Signal'] = [10, 20, 30, 40, 50]
        data = data.merge(signals)
        signals.columns = ['OffsetSecCode', 'OffsetSignal']
        data = data.merge(signals)
        data = data.sort_values(['SecCode', 'zscore']).reset_index(drop=True)
        params = {'pair_offsets': 2, 'signal_thresh_perc': 50,
                  'type': 'tree_model_long'}
        result = _tree_model(scores, data, params)
        benchmark = {'a': -0.375, 'b': -0.125, 'd': 0.125, 'e': 0.375}
        self.assertDictEqual(result, benchmark)
        result = _tree_model(scores, data, params, True)
        benchmark = {'a': -0.25, 'b': -0.25, 'd': 0, 'e': 0.5}
        self.assertDictEqual(result, benchmark)

    def test_tree_model_get_pos_sizes(self):
        data = pd.DataFrame()
        data['SecCode'] = ['a', 'a', 'a', 'b', 'b', 'b']
        data['Signal'] = [1, 1, 1, 2, 2, 2]
        data['zscore'] = [1, 2, 3, 4, 2, 3]
        result = _tree_model_get_pos_sizes(data, 2)
        benchmark = pd.DataFrame(index=[0, 1, 4, 5])  # Index shows filter
        benchmark['SecCode'] = ['a', 'a', 'b', 'b']
        benchmark['Signal'] = [1, 1, 2, 2]
        benchmark['zscore'] = [1, 2, 2, 3]
        benchmark['pos_size'] = [0, 0, 0.5, 0.5]
        benchmark['offset_size'] = [0, 0, -0.5, -0.5]
        assert_frame_equal(result, benchmark)

    def test_tree_model_aggregate_pos_sizes(self):
        data = pd.DataFrame()
        data['SecCode'] = ['a', 'a']
        data['OffsetSecCode'] = ['b', 'c']
        data['pos_size'] = [10, 10]
        data['offset_size'] = [-10, -10]
        result = _tree_model_aggregate_pos_sizes(data)
        benchmark = {'a': 0.5, 'b': -0.25, 'c': -0.25}
        self.assertDictEqual(result, benchmark)
        with self.assertRaises(AssertionError):
            data['pos_size'] = [20, 20]
            result = _tree_model_aggregate_pos_sizes(data)

    def test_basic(self):
        scores = {'A': 0, 'C': 4, 'B': 2, 'D': np.nan}
        result = _basic(scores)
        benchmark = {'A': -0.5, 'B': 0, 'C': 0.5, 'D': 0}
        self.assertDictEqual(result, benchmark)

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
        zscores = pd.DataFrame(index=[dt.date(2010, 1, 1),
                                      dt.date(2010, 1, 2)])
        zscores['A~B'] = [10, 20]
        zscores['A~C'] = [88, 99]
        pair_info = pd.DataFrame()
        pair_info['PrimarySecCode'] = ['A', 'A']
        pair_info['OffsetSecCode'] = ['B', 'C']
        pair_info['distances'] = [99, 98]
        pair_info['distance_rank'] = [1, 2]
        pair_info['pair'] = ['A~B', 'A~C']
        result = _merge_zscores_pair_info(zscores, pair_info)
        benchmark = zscores.unstack().to_frame()
        benchmark.columns = ['zscore']
        benchmark.index.names = ['pair', 'Date']
        benchmark['SecCode'] = ['A', 'A', 'A', 'A']
        benchmark = benchmark.drop('zscore', axis=1)
        benchmark['OffsetSecCode'] = ['B', 'B', 'C', 'C']
        benchmark['distances'] = [99, 99, 98, 98]
        benchmark['distance_rank'] = [1, 1, 2, 2]
        benchmark['zscore'] = [10, 20, 88, 99]
        assert_frame_equal(result, benchmark)

    def test_merge_zscores_signals(self):
        zscores = pd.DataFrame(index=[dt.date(2010, 1, 1),
                                      dt.date(2010, 1, 2)])
        zscores['A~B'] = [10, 20]
        zscores['A~C'] = [88, 99]
        pair_info = pd.DataFrame()
        pair_info['PrimarySecCode'] = ['A', 'A']
        pair_info['OffsetSecCode'] = ['B', 'C']
        pair_info['distances'] = [99, 98]
        pair_info['distance_rank'] = [1, 2]
        pair_info['pair'] = ['A~B', 'A~C']
        zscores = _merge_zscores_pair_info(zscores, pair_info)
        signals = pd.DataFrame()
        signals['SecCode'] = ['A', 'A', 'B', 'B', 'C', 'C', 'D', 'D']
        signals['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 4
        signals['preds'] = range(1000, 1008)
        benchmark = zscores.copy()
        result = _merge_zscores_signals(zscores, signals, pair_info)
        benchmark['Signal'] = [1000, 1001, 1000, 1001]
        benchmark['OffsetSignal'] = [1002, 1003, 1004, 1005]
        assert_frame_equal(result, benchmark)

    def test_final_format(self):
        #import pdb; pdb.set_trace()
        _final_format

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
