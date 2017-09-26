import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.data.data_container_pairs import *


class DataContainerWrapper(DataContainerPairs):
    def _process_data(self, data):
        return data, ['V1', 'V2']


class TestDataContainerPairs(unittest.TestCase):

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
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
