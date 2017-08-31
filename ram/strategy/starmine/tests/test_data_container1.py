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
            'SecCode': ['AAPL'] * 6,
            'Date': dates,
            'AdjClose': [10, 9, 5, 5, 10, 4],
            'RClose': [10, 9, 5, 5, 10, 3],
            'TestFlag': [True] * 6,
            'EARNINGSFLAG': [0, 1, 0, 0, 0, 0]
        })
        self.data['Date'] = convert_date_array(self.data.Date)
        self.DC = DataContainer1()

    def test_trim_training_data(self):
        result = self.DC._trim_training_data(self.data, -99)
        self.assertEqual(len(result), 6)
        result = self.DC._trim_training_data(self.data, 1)
        self.assertEqual(len(result), 2)
    
    def test_get_data_subset(self):
        import ipdb; ipdb.set_trace()
        result = self.DC.get_data_subset(self.data, 1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.Date.iloc[0], dt.date(2015, 4, 1))

        result = self.DC.get_data_subset(self.data, 3)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.Date.iloc[0], dt.date(2015, 7, 1))
    
        df2 = self.data.copy()
        df2['SecCode'] = 'IBM'
        result = self.DC.get_data_subset(self.data.append(df2), 1)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.Date.iloc[0], dt.date(2015, 4, 1))
        self.assertEqual(result.Date.iloc[1], dt.date(2015, 4, 1))


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
