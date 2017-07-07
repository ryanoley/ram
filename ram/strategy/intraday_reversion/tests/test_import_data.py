import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.intraday_reversion.src.import_data import _pivot_data
from ram.strategy.intraday_reversion.src.import_data import _format_returns
from ram.strategy.intraday_reversion.src.import_data import get_available_tickers


class TestImportData(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_available_tickers(self):
        result = get_available_tickers()
        if result != 'No source files or directory found':
            self.assertTrue(True)

    def test_pivot_data(self):
        data = pd.DataFrame()
        data['Date'] = [dt.date(2010, 1, 1)] * 3 + [dt.date(2010, 1, 2)] * 3
        data['Time'] = ['09:31', '09:32', '09:33'] * 2
        data['Close'] = [1, 2, 3, 4, 5, 6.]
        result = _pivot_data(data, 'Close')
        benchmark = pd.DataFrame([[1, 4], [2, 5], [3., 6.]],
                                 index=['09:31', '09:32', '09:33'],
                                 columns=[dt.date(2010, 1, 1),
                                          dt.date(2010, 1, 2)])
        benchmark.index.name = 'Time'
        benchmark.columns.name = 'Date'
        assert_frame_equal(result, benchmark)

        # Introduce NaN value
        data = pd.DataFrame()
        data['Date'] = [dt.date(2010, 1, 1)] * 4 + [dt.date(2010, 1, 2)] * 4
        data['Time'] = ['09:31', '09:32', '09:33', '09:34'] * 2
        data['Close'] = [1, 2, 3, 4, 5, 6, 7, 8.]
        data = data.drop(5)
        result = _pivot_data(data, 'Close')
        benchmark = pd.DataFrame([[1, 5], [2, 5], [3, 7], [4., 8.]],
                                 index=['09:31', '09:32', '09:33', '09:34'],
                                 columns=[dt.date(2010, 1, 1),
                                          dt.date(2010, 1, 2)])
        benchmark.index.name = 'Time'
        benchmark.columns.name = 'Date'
        assert_frame_equal(result, benchmark)

    def test_format_returns(self):
        data = pd.DataFrame()
        data['Ticker'] = ['SPY'] * 10
        data['Date'] = [dt.date(2010, 1, 4)] * 5 + [dt.date(2010, 1, 5)] * 5
        data['Time'] = [dt.time(10, i) for i in range(5)] * 2
        data['Open'] = [10] * 10
        data['High'] = [10, 11, 12, 13, 14] * 2
        data['Low'] = [10, 9, 8, 7, 6] * 2
        data['Close'] = [11] * 10
        result = _format_returns(data)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
