import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.intraday_reversion.src.take_stop_returns import get_long_returns
from ram.strategy.intraday_reversion.src.take_stop_returns import get_short_returns
from ram.strategy.intraday_reversion.src.take_stop_returns import _get_first_time_index_by_column
from ram.strategy.intraday_reversion.src.take_stop_returns import _calculate_rets
from ram.strategy.intraday_reversion.src.import_data import _format_returns


class TestTakeStopReturns(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame()
        data['Ticker'] = ['SPY'] * 10
        data['Date'] = [dt.date(2010, 1, 4)] * 5 + [dt.date(2010, 1, 5)] * 5
        data['Time'] = [dt.time(10, i) for i in range(5)] * 2
        data['Open'] = [10] * 10
        data['High'] = [10, 13, 12, 13, 14] + [10, 10, 9, 10, 8]
        data['Low'] = [10, 10, 10, 8, 11] + [10, 9, 8, 6, 2]
        data['Close'] = [11] * 10
        self.data = data

    def test_get_first_time_index_by_column(self):
        high_rets, low_rets, close_rets = _format_returns(self.data)
        result = _get_first_time_index_by_column(high_rets > 0.1)
        benchmark = pd.Series([dt.datetime(1950, 1, 1, 10, 1),
                               dt.datetime(1950, 1, 1, 10, 4)])
        benchmark.index = [dt.date(2010, 1, 4), dt.date(2010, 1, 5)]
        benchmark.name = 'Time'
        benchmark.index.name = 'Date'
        assert_series_equal(pd.to_datetime(result), pd.to_datetime(benchmark))
        #
        result = _get_first_time_index_by_column(low_rets < -0.1)
        benchmark = pd.Series([dt.datetime(1950, 1, 1, 10, 3),
                               dt.datetime(1950, 1, 1, 10, 2)])
        benchmark.index = [dt.date(2010, 1, 4), dt.date(2010, 1, 5)]
        benchmark.name = 'Time'
        benchmark.index.name = 'Date'
        assert_series_equal(pd.to_datetime(result), pd.to_datetime(benchmark))

    def test_calculate_rets(self):
        high_rets, low_rets, close_rets = _format_returns(self.data)
        # Simulate a long position
        take_perc = 0.2
        stop_perc = 0.1
        wins = _get_first_time_index_by_column(high_rets > take_perc)
        losses = _get_first_time_index_by_column(low_rets < -stop_perc)
        result = _calculate_rets(wins, losses, close_rets, take_perc, stop_perc)
        benchmark = pd.Series([0.2, -0.1])
        benchmark.name = 'Rets'
        benchmark.index = [dt.date(2010, 1, 4), dt.date(2010, 1, 5)]
        benchmark.index.name = 'Date'
        assert_series_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
