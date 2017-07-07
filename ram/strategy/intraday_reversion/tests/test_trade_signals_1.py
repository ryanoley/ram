import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal


class TestTradeSignals1(unittest.TestCase):

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

    def Xtest_get_trade_signals(self):
        data = pd.DataFrame()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
