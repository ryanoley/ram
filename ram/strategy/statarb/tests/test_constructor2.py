import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.constructor2 import PortfolioConstructor2


class TestConstructor(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1), dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3), dt.datetime(2015, 1, 4)]
        self.scores = pd.DataFrame({
            'AAPL~GOOGL': [2, 0, 4, 0],
            'AAPL~IBM': [0, 0, 3, 1],
            'GOOGL~IBM': [0, -2, -3, -2],
        }, index=dates)

        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'Date': dates * 3,
            'AdjClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RCashDividend': [0] * 12,
            'SplitFactor': [1] * 12
        })
        self.pair_info = pd.DataFrame({})

    def test_split_position_and_count(self):
        result = PortfolioConstructor2._split_position_and_count(
            (10, 'ABC_DEF~GHI_JKL', -1))
        benchmark = {'ABC': -1, 'DEF': -1, 'GHI': 1, 'JKL': 1}
        self.assertDictEqual(result, benchmark)
        try:
            PortfolioConstructor2._split_position_and_count(
                (10, 'ABC_DEF~ABC_JKL', -1))
        except AssertionError as e:
            self.assertEqual(e.message, 'Duplicate SecCodes in position')
        except:
            self.assertTrue(False)

    def test_can_add(self):
        counts = {'P2': -1, 'P4': 1}
        port_counts = {'P1': 1, 'P2': -2, 'P3': 1}
        result = PortfolioConstructor2._can_add(counts, port_counts,
                                                max_pos_count=2)
        self.assertFalse(result)
        counts = {'P1': -1, 'P4': 1}
        port_counts = {'P1': 1, 'P2': -2, 'P3': 1}
        result = PortfolioConstructor2._can_add(counts, port_counts,
                                                max_pos_count=2)
        self.assertTrue(result)

    def test_add_to_port(self):
        counts = {'P1': -1, 'P4': 1}
        port_counts = {'P1': 1, 'P2': -2, 'P3': 1}
        result = PortfolioConstructor2._add_to_port(counts, port_counts)
        benchmark = {'P1': 0, 'P2': -2, 'P3': 1, 'P4': 1}
        self.assertDictEqual(result, benchmark)

    def test_get_top_x_positions(self):
        positions = [(10, 'ABC~DEF', -1), (9, 'ABC~GHI', -1),
                     (8, 'ABC~JKL', -1), (7, 'ABC~MNO', 1),
                     (6, 'DEF~GHI', -1), (5, 'DEF~JKL', -1),
                     (4, 'DEF~MNO', -1)]
        result = PortfolioConstructor2()._get_top_x_positions(
            positions, n_pairs=3, max_pos_count=2)
        benchmark = [(10, 'ABC~DEF', -1),
                     (9, 'ABC~GHI', -1),
                     (7, 'ABC~MNO', 1)]
        self.assertListEqual(result, benchmark)
        positions = [(10, 'ABC~DEF', -1),
                     (6, 'DEF~GHI', 1), (5, 'ABD~DEF', -1),
                     (4, 'DEF~MNO', -1)]
        result = PortfolioConstructor2()._get_top_x_positions(
            positions, n_pairs=3, max_pos_count=2)
        benchmark = [(10, 'ABC~DEF', -1), (6, 'DEF~GHI', 1),
                     (4, 'DEF~MNO', -1)]
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
