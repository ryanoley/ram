import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.data.dh_file import DataHandlerFile


class TestDataHandlerFile(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame({
            'SecCode': ['a']*10 + ['b']*10 + ['c']*10,
            'Date': ['2016-01-{0:02d}'.format(x) for x in range(1, 11)] * 3,
            'Close': range(1, 31),
            'AvgDolVol': [200]*10 + [100]*10 + [300]*10
            }, columns=['Date', 'SecCode', 'Close', 'AvgDolVol'])
        self.data = data
        self.dh = DataHandlerFile(data)

    def test_get_id_data(self):
        result = self.dh.get_id_data(
            ids=['a', 'b'],
            features='Close',
            start_date=dt.date(2016, 1, 3),
            end_date='2016-01-08')
        benchmark = self.data.copy()
        benchmark = benchmark.drop('AvgDolVol', axis=1)
        benchmark = benchmark.loc[[2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 17]]
        benchmark = benchmark.reset_index(drop=True)
        assert_frame_equal(result, benchmark)

    def test_get_filtered_univ_data(self):
        result = self.dh.get_filtered_univ_data(
            univ_size=2,
            features=['Close', 'AvgDolVol'],
            start_date=dt.date(2016, 1, 3),
            filter_date='2016-01-05',
            end_date='2016-01-08',
            filter_column='AvgDolVol',
            filter_bad_ids=False)
        benchmark = self.data.copy()
        benchmark = benchmark.loc[[2, 3, 4, 5, 6, 7, 22, 23, 24, 25, 26, 27]]
        benchmark = benchmark.reset_index(drop=True)
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
