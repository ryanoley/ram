import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.birds.signals.signals1 import Signals1


class TestSignals1(unittest.TestCase):

    def setUp(self):
        self.data = pd.DataFrame({
            'SecCode': ['a'] * 3 + ['b'] * 3 + ['c'] * 3 + ['d'] * 3,
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'] * 4,
            'V1': [9, 9, 5,
                   1, 5, 1,
                   5, 1, 5,
                   5, 5, 9],
            'V2': [5, 5, 1,
                   1, 1, 5,
                   9, 5, 5,
                   5, 9, 9]
        })[['SecCode', 'Date', 'V1', 'V2']]

    def test_generate_portfolio_signals(self):
        signals = Signals1()
        signals.register_index_variables(['V1', 'V2'])
        result = signals.generate_portfolio_signals(self.data)
        benchmark = self.data.copy()
        benchmark.V1 = [1, 1, 0, -1, 0, -1, 0, -1, 0, 0, 0, 1]
        benchmark.V2 = [0, 0, -1, -1, -1, 0, 1, 0, 0, 0, 1, 1]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':

    unittest.main()
