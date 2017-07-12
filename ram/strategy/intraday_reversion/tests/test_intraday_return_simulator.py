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

    def test_get_returns(self):
        irs = IntradayReturnSimulator()
        returns_df = pd.DataFrame()
        returns_df['Date'] = [
            dt.date(2010, 1, 1), dt.date(2010, 1, 2), dt.date(2010, 1, 3),
            dt.date(2010, 1, 4), dt.date(2010, 1, 5)] * 2
        returns_df['Ticker'] = 'SPY'
        returns_df['Return'] = [10, 10, -10, 10, 10] + [5, -5, 5, -5, 5]
        returns_df['perc_take'] = [0.004] * 5 + [0.008] * 5
        returns_df['perc_stop'] = [0.002] * 10
        returns_df['signal'] = 1
        returns_df2 = returns_df.copy()
        returns_df2.Return *= -3
        returns_df2.signal = -1
        returns_df = returns_df.append(returns_df2).reset_index(True)
        irs._return_data = {}
        irs._return_data['SPY'] = returns_df
        #
        signals = self.data.copy()
        signals['perc_take'] = [0.004, 0.008, 0.004, 0.004, 0.008]
        signals['perc_stop'] = 0.002
        signals['signal'] = [0, -1, 1, 0, -1]
        result = irs.get_returns(signals)
        benchmark = pd.Series([0, 15, -10, 0, -15.],
            index=[dt.date(2010, 1, 1), dt.date(2010, 1, 2),
                   dt.date(2010, 1, 3), dt.date(2010, 1, 4),
                   dt.date(2010, 1, 5)])
        benchmark.index.name = 'Date'
        assert_series_equal(result[0], benchmark)

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

    def Xtest_preprocess_returns(self):
        irs = IntradayReturnSimulator()
        import pdb; pdb.set_trace()
        irs.preprocess_returns([0.004, 0.008], 0.002, 'SPY')

    def test_get_ticker_stats(self):
        returns = pd.DataFrame(index=[dt.date(2009, 12, 31),
                                      dt.date(2010, 1, 1),
                                      dt.date(2010, 1, 2),
                                      dt.date(2010, 1, 3),
                                      dt.date(2010, 1, 4)])
        returns['SPY'] = [0, 10, 10, -10, -10]
        returns['VXX'] = [0, np.nan, 10, 0, 0]
        returns['IWM'] = [0, 1, 2, 0, np.nan]
        irs = IntradayReturnSimulator()
        result = irs._get_ticker_stats(returns)
        self.assertAlmostEqual(result['win_percent_SPY'], 1/2.)
        self.assertAlmostEqual(result['win_percent_IWM'], 1)
        self.assertAlmostEqual(result['win_percent_VXX'], 1.0)
        self.assertAlmostEqual(result['participation_SPY'], 1.0)
        self.assertAlmostEqual(result['participation_IWM'], 2/3.)
        self.assertAlmostEqual(result['participation_VXX'], 1/3.)
        self.assertAlmostEqual(result['total_return_SPY'], 0)
        self.assertAlmostEqual(result['total_return_IWM'], 3)
        self.assertAlmostEqual(result['total_return_VXX'], 10)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
