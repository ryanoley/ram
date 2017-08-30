import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.data.data_container1 import *


class DataContainerWrapper(DataContainer1):
    def _process_data(self, data):
        return data, ['V1', 'V2']


class TestDataContainer1(unittest.TestCase):

    def setUp(self):
        dates = ['2015-03-29', '2015-03-30', '2015-03-31',
                 '2015-04-01', '2015-04-02', '2015-04-03']
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6,
            'Date': dates,
            'AdjClose': [10, 9, 5, 5, 10, 4],
            'RClose': [10, 9, 5, 5, 10, 3],
            'V1': range(6),
            'V2': range(1, 7),
            'TestFlag': [False] * 4 + [True] * 2
        })
        self.data['Date'] = convert_date_array(self.data.Date)
        self.data2 = self.data.copy()
        self.data2.Date = ['2015-02-01', '2015-02-02', '2015-02-03',
                           '2015-02-04', '2015-02-05', '2015-02-06']
        self.data2['Date'] = convert_date_array(self.data2.Date)

    def test_get_response_data(self):
        container = DataContainerWrapper()
        container.add_data(self.data, 1)
        result = container._get_response_data(1, 2, 0.5)
        self.assertEqual(result.shape[0], 6)
        # Test that it stacked
        container.add_data(self.data2, 2)
        result = container._get_response_data(2, 2, 0.5)
        self.assertEqual(result.shape[0], 10)

    def Xtest_prep_data(self):
        container = DataContainerWrapper()
        container.add_data(self.data, 1)
        container.prep_data(1, 2, .5, -99)

    def test_trim_training_data(self):
        result = DataContainer1()._trim_training_data(self.data, -99)
        self.assertEqual(len(result), 6)
        result = DataContainer1()._trim_training_data(self.data, 1)
        self.assertEqual(len(result), 3)

    def test_make_weekly_monthly_indexes(self):

        dates = ['2015-03-28', '2015-03-29', '2015-03-30', '2015-03-31',
                 '2015-04-01', '2015-04-02', '2015-04-03', '2015-04-04',
                 '2015-04-05', '2015-04-06', '2015-04-07']
        data = pd.DataFrame({
            'SecCode': ['AAPL'] * 11,
            'Date': dates,
            'Response': [1] * 11,
            'TestFlag': [False] * 4 + [True] * 7
        })
        data['Date'] = convert_date_array(data.Date)
        result = make_weekly_monthly_indexes(data, 2)
        benchmark = pd.DataFrame()
        benchmark['Date'] = data.Date.copy()
        benchmark['month_index'] = [0] * 2 + [1.] * 9
        benchmark['week_index'] = [0] * 4 + [1] * 5 + [2.] * 2
        benchmark['week_index_train_offset'] = [0] * 2 + [1] * 5 + [2.] * 4
        assert_frame_equal(benchmark, result)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
