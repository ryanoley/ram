import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.data.data_container1 import *
from ram.strategy.long_pead.data.data_container1 import \
    _clean_impute_data_with_train_test


class TestDataContainer1(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-03-01', '2015-04-01',
                 '2015-05-04', '2015-07-01', '2015-08-01']
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6,
            'Date': dates,
            'AdjClose': [10, 9, 5, 5, 10, 4],
            'RClose': [10, 9, 5, 5, 10, 3],
            'TestFlag': [True] * 6
        })
        self.data['Date'] = convert_date_array(self.data.Date)

    def test_trim_training_data(self):
        result = DataContainer1()._trim_training_data(self.data, -99)
        self.assertEqual(len(result), 6)
        result = DataContainer1()._trim_training_data(self.data, 1)
        self.assertEqual(len(result), 2)

    def test_clean_impute_data_with_train_test(self):
        data = pd.DataFrame({
            'V1': [1, 3, np.nan, np.nan, np.nan],
            'V2': [2, 6, np.nan, 10, np.nan],
            'V3': [np.nan] * 5,
            'TestFlag': [False, False, False, True, True]
        })
        features = ['V1', 'V2', 'V3']
        result = _clean_impute_data_with_train_test(data, features)
        benchmark = data.copy()
        benchmark['V1'] = [1, 3, 2, 2, 2.]
        benchmark['V2'] = [2, 6, 4, 10, 4.]
        benchmark['V3'] = 0.
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
