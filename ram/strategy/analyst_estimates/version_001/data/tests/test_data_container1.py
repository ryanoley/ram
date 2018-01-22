import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.analyst_estimates.version_001.data.data_container1 import DataContainer1

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_frame_equal


class TestDataContainer1(unittest.TestCase):

    def setUp(self):
        dates = [dt.date(2015,1,1), dt.date(2015,3,1), dt.date(2015,4,1),
                    dt.date(2015,5,4), dt.date(2015,7,1), dt.date(2015,8,1)]
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6 + ['BAC'] * 6,
            'Date': dates * 2,
            'EARNINGSFLAG': [0, 1, 0, 0, 0, 0] * 2,
            'GGROUP': [4510.] * 6 + [2510.] * 6
        })
        self.dc = DataContainer1()

    def test_trim_training_data(self):
        result = self.dc._trim_training_data(self.data, -99)
        assert_frame_equal(result, self.data)

        result = self.dc._trim_training_data(self.data, 1)
        benchmark = self.data[self.data.Date >= dt.date(2015, 7, 1)]
        benchmark.reset_index(drop=True, inplace=True)

        assert_frame_equal(result, benchmark)

    def test_make_group_dict(self):
        result = self.dc._make_group_dict(self.data)
        benchmark = {'AAPL': [4510.], 'BAC': [2510.]}
        self.assertEqual(result, benchmark)

        inp_data = self.data.copy()
        inp_data.loc[[0, 1], 'GGROUP'] = np.nan
        result = self.dc._make_group_dict(inp_data)
        self.assertEqual(result, benchmark)

        inp_data = self.data.copy()
        inp_data.loc[[0, 1], 'GGROUP'] = 4520
        benchmark = {'AAPL': [4520., 4510.], 'BAC': [2510.]}
        result = self.dc._make_group_dict(inp_data)
        self.assertEqual(result, benchmark)

    def test_filter_entry_window(self):
        result = self.dc._filter_entry_window(self.data, 1)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.Date.iloc[0], dt.date(2015, 3, 1))

        result = self.dc._filter_entry_window(self.data, 3)
        self.assertEqual(len(result), 6)
        assert_array_equal(result['T'].values, np.array([0, 1 , 2] * 2))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

