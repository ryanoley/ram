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

    def test_get_returns(self):
        import pdb; pdb.set_trace()
        irs = IntradayReturnSimulator()
        result = irs.get_returns(self.data)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
