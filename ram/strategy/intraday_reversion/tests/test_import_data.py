import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.intraday_reversion.src.import_data import _pivot_data
from ram.strategy.intraday_reversion.src.import_data import _format_hlc_returns
from ram.strategy.intraday_reversion.src.import_data import _format_costs
from ram.strategy.intraday_reversion.src.import_data import get_available_tickers


class TestImportData(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_available_tickers(self):
        result = get_available_tickers()
        if result == 'No source files or directory found':
            self.skipTest('Could not connect to server')
        else:
            self.assertTrue('SPY' in result)

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

        # Introduce NaN value
        data = pd.DataFrame()
        data['Date'] = [dt.date(2010, 1, 1)] * 4 + [dt.date(2010, 1, 2)] * 4
        data['Time'] = ['09:31', '09:32', '09:33', '09:34'] * 2
        data['Close'] = [1, 2, 3, 4, 5, 6, 7, 8.]
        data = data.drop(0)
        result = _pivot_data(data, 'Close')
        benchmark = pd.DataFrame([[2, 5], [2, 6], [3, 7], [4., 8.]],
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
        result = _format_hlc_returns(data)
        self.assertTrue(isinstance(result[0], pd.DataFrame))
        self.assertTrue(isinstance(result[1], pd.DataFrame))
        self.assertTrue(isinstance(result[2], pd.DataFrame))

        benchmark_high_ret = np.array([0.,.1,.2,.3,.4])
        result_high_ret = result[0][dt.date(2010,1,4)].values
        assert_array_equal(np.round(result_high_ret, 5),
                           np.round(benchmark_high_ret, 5))

        benchmark_low_ret = np.array([0.,-.1,-.2,-.3,-.4])
        result_low_ret = result[1][dt.date(2010,1,4)].values
        assert_array_equal(np.round(result_low_ret, 5),
                           np.round(benchmark_low_ret, 5))

        benchmark_close_ret = np.array([.1,.1,.1,.1,.1])
        result_close_ret = result[2][dt.date(2010,1,4)].values
        assert_array_equal(np.round(result_close_ret, 5),
                           np.round(benchmark_close_ret, 5))

    def test_format_costs(self):
        data = pd.DataFrame()
        data['Ticker'] = ['SPY'] * 10
        data['Date'] = [dt.date(2010, 1, 4)] * 5 + [dt.date(2010, 1, 5)] * 5
        data['Time'] = [dt.time(10, i) for i in range(5)] * 2
        data['Open'] = [10] * 10
        data['High'] = [10, 11, 12, 13, 14] * 2
        data['Low'] = [10, 9, 8, 7, 6] * 2
        data['Close'] = [11] * 10
        result = _format_costs(data, 0.02, 0.01)
        self.assertTrue(isinstance(result[0], pd.Series))
        self.assertTrue(isinstance(result[1], pd.Series))
        self.assertEqual(result[0].iloc[0], 0.002)
        self.assertEqual(result[1].iloc[0], 0.001)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
