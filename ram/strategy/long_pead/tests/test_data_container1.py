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
        dates = ['2015-01-01', '2015-03-01', '2015-04-01',
                 '2015-05-01', '2015-07-01', '2015-08-01']
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

    def test_get_response_data(self):
        container = DataContainerWrapper()
        container.add_data(self.data, 1)
        result = container._get_response_data(1, 2, 0.5)
        self.assertEqual(result.shape[0], 4)
        # Test that it stacked
        container.add_data(self.data, 2)
        result = container._get_response_data(2, 2, 0.5)
        self.assertEqual(result.shape[0], 8)

    def test_prep_data(self):
        container = DataContainerWrapper()
        container.add_data(self.data, 1)
        container.prep_data(1, 2, .5, -99)

    def test_trim_training_data(self):
        result = DataContainer1()._trim_training_data(self.data, -99)
        self.assertEqual(len(result), 6)
        result = DataContainer1()._trim_training_data(self.data, 1)
        self.assertEqual(len(result), 2)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
