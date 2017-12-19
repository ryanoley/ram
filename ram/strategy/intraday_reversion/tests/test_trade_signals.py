import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.intraday_reversion.src.trade_signals import prediction_thresh_optim
from ram.strategy.intraday_reversion.src.trade_signals import _get_prediction_thresh
from ram.strategy.intraday_reversion.src.trade_signals import _get_trade_signals

class TestPredictionThreshOptim(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_prediction_thresh_optim(self):
        data = pd.DataFrame()
        data['Ticker'] = ['SPY'] * 3 + ['VXX'] * 3 + ['IWM'] * 3
        data['Date'] = dt.date(2010, 1, 1)
        data['prediction'] = [4, 5, 2, 6, 1, 7, 6, 5, 0]
        data['zOpen'] = 10
        data['response'] = [0, 1, 0, -1, -1, 1, 0, 1, 0]
        # Add gap down side
        data2 = data.copy()
        data2['zOpen'] = -10
        data = data.append(data2).reset_index(drop=True)
        # Add row for extra dates
        data2 = data.iloc[:2].copy()
        data2['Date'] = dt.date(2010, 1, 2)
        data = data.append(data2).reset_index(drop=True)
        result = prediction_thresh_optim(data, 1, 0.2, 0.3, 0.2, 0.3)

    def test_get_prediction_thresh(self):
        data = pd.DataFrame()
        data['Ticker'] = ['SPY'] * 3 + ['VXX'] * 3 + ['IWM'] * 3
        data['Date'] = [dt.date(2010, 1, i) for i in range(1, 4)] * 3
        data['prediction'] = [4, 5, 2, 6, 1, 7, 6, 5, 0]
        data['zOpen'] = 10
        data['response'] = [0, 1, 0, -1, -1, 1, 0, 1, 0]
        data = data.sort_values('prediction')
        result = _get_prediction_thresh(data, 0.2, 0.3)
        self.assertEqual(result, 1)

    def test_get_trade_signals(self):
        data = pd.DataFrame()
        data['Ticker'] = ['SPY'] * 4
        data['Date'] = dt.date(2010, 1, 1)
        data['gap_down_inflection'] = 4
        data['gap_up_inflection'] = 4
        data['zOpen'] = [10, 10, -10, -10]
        data['prediction'] = [10, -10, 10, -10]
        result = _get_trade_signals(data, 1)
        benchmark = np.array([1, -1, 1, -1])
        assert_array_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
