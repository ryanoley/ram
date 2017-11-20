import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.version_001.data.pairs_selector_filter import \
    PairSelectorFilter


class TestPairSelectorFilter(unittest.TestCase):

    def setUp(self):
        pair_info = pd.DataFrame()
        pair_info['Leg1'] = ['A', 'A', 'A', 'B']
        pair_info['Leg2'] = ['B', 'C', 'D', 'C']
        pair_info['distances'] = [10, 20, 30, 10]
        self.pair_info = pair_info
        spreads = pd.DataFrame(
            index=[dt.date(2010, 1, 1), dt.date(2010, 1, 2)])
        spreads['A~B'] = [10, -10]
        spreads['A~C'] = [20, 30]
        spreads['A~D'] = [20, 30]
        spreads['B~C'] = [20, 30]
        self.spreads = spreads

    def test_filter(self):
        # DATA
        zscores = self.spreads.copy()
        # Pair Selector
        psf = PairSelectorFilter(3)
        result = psf.filter(self.pair_info, self.spreads, zscores)

    def test_get_top_n_pairs_per_seccode(self):
        psf = PairSelectorFilter(2)
        result = psf._get_top_n_pairs_per_seccode(self.pair_info)
        benchmark = ['A', 'A', 'B', 'B', 'C', 'C', 'D']
        self.assertListEqual(result.Leg1.tolist(), benchmark)

    def test_double_flip_frame(self):
        psf = PairSelectorFilter(2)
        result = psf._double_flip_frame(self.spreads)
        benchmark = ['A~B', 'A~C', 'A~D', 'B~C', 'B~A', 'C~A', 'D~A', 'C~B']
        self.assertListEqual(result.columns.tolist(), benchmark)
        self.assertEqual(result.shape[0], 2)
        benchmark = self.spreads.sum().sum()
        col_num = self.spreads.shape[1]
        self.assertEqual(result.iloc[:, :col_num].sum().sum(), benchmark)
        self.assertEqual(result.iloc[:, col_num:].sum().sum(), -benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
