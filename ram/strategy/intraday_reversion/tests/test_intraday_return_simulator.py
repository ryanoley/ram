import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.intraday_reversion.src.intraday_return_simulator import *


class TestIntradayReturnSimulator(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame([])
        data['Ticker'] = ['SPY'] * 5
        data['Date'] = [dt.date(2010, 1, i) for i in range(1, 6)]
        data['signal'] = [0, 0, 1, 0, 1]
        self.data = data
        data2 = pd.DataFrame([[0, 0, 0], [1, -1, 0.01],
                              [2, -2, 0.01], [-3., 3., 0.01]],
                             index=[dt.time(9, 31), dt.time(9, 32),
                                    dt.time(9, 33), dt.time(9, 34)],
                             columns=[dt.date(2010, 1, 1),
                                      dt.date(2010, 1, 2),
                                      dt.date(2010, 1, 3)])
        data2.index.name = 'Time'
        data2.columns.name = 'Date'
        self.data2 = data2

    def test_get_returns_from_signals(self):
        irs = IntradayReturnSimulator()
        longs = pd.Series([10, 10, -10, 10],
                          index=[dt.date(2010, 1, i) for i in range(2, 6)])
        shorts = pd.Series([-20, -20, 20, -20],
                           index=[dt.date(2010, 1, i) for i in range(2, 6)])
        signals = pd.DataFrame({
            'Ticker': ['SPY'] * 6,
            'Date': [dt.date(2010, 1, i) for i in range(1, 7)],
            'signal': [1, 1, -1, 1, -1, 1]
        })
        result = irs._get_returns_from_signals(signals, longs, shorts)
        benchmark = pd.Series(
            [0, 10, -20, -10, -20, 0.], name='SPY',
            index=[dt.date(2010, 1, i) for i in range(1, 7)])
        benchmark.index.name = 'Date'
        assert_series_equal(result, benchmark)

    def test_get_responses(self):
        irs = IntradayReturnSimulator()
        # Add test bar data
        irs._bar_data['SPY'] = (self.data2, self.data2, self.data2)
        result = irs.get_responses('SPY', 1, .3)
        benchmark = pd.DataFrame(index=[dt.date(2010, 1, 1),
                                        dt.date(2010, 1, 2),
                                        dt.date(2010, 1, 3)])
        benchmark.index.name = 'Date'
        benchmark['Ticker'] = 'SPY'
        benchmark['response'] = [1, -1, 0]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
