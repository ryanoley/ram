import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.birds.data import *

from pandas.util.testing import assert_frame_equal


class TestData(unittest.TestCase):

    def setUp(self):
        test_dates = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]]
        data = pd.DataFrame(index=test_dates)
        data['A'] = [10, 10, 20, 20, 30]
        data['B'] = [10, 10, 20, 20, 30]
        data['C'] = [20, 20, 30, 30, 10]
        data['D'] = [20, 20, 30, 30, 10]
        data['E'] = [30, 30, 10, 10, 20]
        data['F'] = [30, 30, 10, 10, 20]
        self.data = data

    def test_make_groups(self):
        result = make_groups(self.data, n_groups=3, n_days=2)
        self.data.iloc[-1] = self.data.iloc[-2]
        assert_frame_equal(result, self.data / 10.)

    def test_get_index_features(self):
        groups = make_groups(self.data, n_groups=3, n_days=2)
        result = get_index_features(self.data, groups)
        benchmark = pd.DataFrame()
        benchmark['Group'] = [1.0] * 5 + [2.0] * 5 + [3.0] * 5
        benchmark['Date'] = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]] * 3
        benchmark['Feature'] = [10, 10, 10, 10, 20,
                                20, 20, 20, 20, 30,
                                30, 30, 30, 30, 10]
        assert_frame_equal(result, benchmark)

    def test_get_index_returns(self):
        groups = make_groups(self.data, n_groups=3, n_days=2)
        result = get_index_returns(self.data.pct_change(), groups)
        benchmark = pd.DataFrame()
        benchmark['Group'] = [1.0] * 4 + [2.0] * 4 + [3.0] * 4
        benchmark['Date'] = [dt.date(2010, 1, i) for i in [2, 3, 4, 5]] * 3
        benchmark['DailyReturn'] = [0, 1, 0, 1, 0, .5, 0,
                                    .5, 0, -2/3., 0, -2/3.]
        assert_frame_equal(result, benchmark)

    def test_get_index_responses(self):
        groups = make_groups(self.data, n_groups=3, n_days=2)
        features = get_index_features(self.data, groups)
        returns = get_index_returns(self.data.pct_change(), groups)
        features = features.merge(returns, how='left')
        result = get_index_responses(features, n_days=2)
        benchmark = pd.DataFrame()
        benchmark['Group'] = [1.0] * 5 + [2.0] * 5 + [3.0] * 5
        benchmark['Date'] = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]] * 3
        benchmark['Response'] = [1, 1, 1, np.nan, np.nan, 1, 1, 1,
                                 np.nan, np.nan, 0, 0, 0., np.nan, np.nan]
        assert_frame_equal(result, benchmark)

    def test_make_indexes(self):
        result = make_indexes(data=self.data,
                              close_prices=self.data,
                              test_dates=self.data.index,
                              label='V1',
                              n_groups=3,
                              n_days=2)
        benchmark = pd.DataFrame()
        benchmark['Group'] = ['V1_1'] * 5 + ['V1_2'] * 5 + ['V1_3'] * 5
        benchmark['Date'] = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]] * 3
        benchmark['Feature'] = [10, 10, 10, 10, np.nan,
                                20, 20, 20, 20, np.nan,
                                30, 30, 30, 30, np.nan]
        benchmark['DailyReturn'] = [np.nan, 0, 1, 0, 1,
                                    np.nan, 0, .5, 0, .5,
                                    np.nan, 0, -2/3., 0, -2/3.]
        benchmark['Response'] = [1, 1, 1, np.nan, np.nan, 1, 1, 1,
                                 np.nan, np.nan, 0, 0, 0., np.nan, np.nan]
        assert_frame_equal(result, benchmark)

    def test_extract_test_dates(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
