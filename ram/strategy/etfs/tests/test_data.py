import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.etfs.data import ETFData


class TestETFData(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1), dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3), dt.datetime(2015, 1, 4)]
        data = pd.DataFrame({
            'Open': [1, 2, 3, 4],
            'High': [1, 2, 3, 4],
            'Low': [1, 2, 3, 4],
            'Close': [1, 2, 3, 4],
            'Volume': [1, 2, 3, 4],
            'AdjClose': [1, 2, 3, 4]
        }, index=dates)
        data.index.name = 'Date'
        self.data = data

    def test_init(self):
        import pdb; pdb.set_trace()
        etf_data = ETFData('SPY')
        etf_data.data = self.data

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
