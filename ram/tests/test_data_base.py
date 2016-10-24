import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.data.base import DataHandler


class TestDataHandler(unittest.TestCase):

    def setUp(self):
        self.data = pd.DataFrame({
            'ID': ['a']*10 + ['b']*10 + ['c']*10,
            'Date': [x for x in pd.date_range('2016-01-01', '2016-01-10')] * 3,
            'Close': range(1, 31),
            'AvgDolVol': [100]*10 + [200]*10 + [300]*10
            }, columns=['Date', 'ID', 'Close', 'AvgDolVol'])

    def test_init(self):
        import pdb; pdb.set_trace()
        dh = DataHandler(self.data)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
