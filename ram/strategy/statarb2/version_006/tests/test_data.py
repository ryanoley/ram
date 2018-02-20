import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb2.version_006.data import *

from pandas.util.testing import assert_series_equal, assert_frame_equal


class TestData(unittest.TestCase):

    def setUp(self):
        pass

    def test_rank_filter_data(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 4 + ['B'] * 4 + ['C'] * 4
        data['Date'] = [dt.date(2010, 1, i) for i in range(1, 5)] * 3
        data['V1'] = range(12)
        inds = np.array([True] * 6 + [False, False] + [True] * 4)
        result = rank_filter_data(data, 'V1', inds)
        benchmark = pd.DataFrame(index=data.Date.unique())
        benchmark['A'] = [1/3., 1/3., 1/2., 1/2.]
        benchmark['B'] = [2/3., 2/3., np.nan, np.nan]
        benchmark['C'] = [1.0] * 4
        benchmark.columns.name = 'SecCode'
        benchmark.index.name = 'Date'
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
