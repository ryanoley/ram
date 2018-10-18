import unittest
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal
from ram.strategy.sandbox.version_001.data.data_container1 import DataContainer1


class TestDataContainer1(unittest.TestCase):

    def setUp(self):
        dates = [dt.date(2015,1,1), dt.date(2015,3,1), dt.date(2015,4,1),
                    dt.date(2015,5,4), dt.date(2015,7,1), dt.date(2015,8,1)]
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6 + ['BAC'] * 6,
            'Date': dates * 2,
            'SplitFactor': [2, 2, 1, .5, 1, 1] * 2
        })
        self.dc = DataContainer1()

    def test_trim_training_data(self):
        result = self.dc._trim_training_data(self.data, -99)
        assert_frame_equal(result, self.data)

        result = self.dc._trim_training_data(self.data, 1)
        benchmark = self.data[self.data.Date >= dt.date(2015, 7, 1)]
        benchmark.reset_index(drop=True, inplace=True)

        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
