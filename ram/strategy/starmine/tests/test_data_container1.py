import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.starmine.data.data_container1 import DataContainer1


class TestDataContainer1(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-03-01', '2015-04-01',
                 '2015-05-04', '2015-07-01', '2015-08-01']
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6 + ['BAC'] * 6,
            'Date': dates * 2,
            'EARNINGSFLAG': [0, 1, 0, 0, 0, 0] * 2,
            'GGROUP': [4510.] * 6 + [2510.] * 6
        })
        self.data['Date'] = convert_date_array(self.data.Date)
        self.dc = DataContainer1()

    def test_trim_training_data(self):
        result = self.dc._trim_training_data(self.data, -99)
        self.assertEqual(len(result), 12)
        result = self.dc._trim_training_data(self.data, 1)
        self.assertEqual(len(result), 4)

    def test_filter_entry_window(self):
        result = self.dc._filter_entry_window(self.data, 1)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.Date.iloc[0], dt.date(2015, 3, 1))

        result = self.dc._filter_entry_window(self.data, 3)
        self.assertEqual(len(result), 6)
        assert_array_equal(result['T'].values, np.array([0, 1 , 2] * 2))

    def test_make_exit_dict(self):
        self.dc._entry_window = 2
        result = self.dc._make_exit_dict(self.data, response_days = 2)

        benchmark = {
            1: {
                dt.date(2015, 7, 1): ['AAPL', 'BAC'],
                dt.date(2015, 8, 1): ['AAPL', 'BAC']
                },
            2: {
                dt.date(2015, 8, 1): ['AAPL', 'BAC']
                }
            }

        self.assertEqual(result, benchmark)

    def test_make_group_dict(self):
        result = self.dc._make_group_dict(self.data)
        benchmark = {'AAPL': [4510.], 'BAC': [2510.]}
        
        inp_data = self.data.copy()
        inp_data.loc[[0, 1], 'GGROUP'] = np.nan
        result = self.dc._make_group_dict(inp_data)
        self.assertEqual(result, benchmark)


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
