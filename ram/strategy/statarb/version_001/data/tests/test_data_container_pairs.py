import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.statarb.version_001.data.data_container_pairs import *


class TestDataContainerPairs(unittest.TestCase):

    def setUp(self):
        dates = ['2015-03-29', '2015-03-30', '2015-03-31',
                 '2015-04-01', '2015-04-02', '2015-04-03']
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6,
            'Date': dates,
            'AdjClose': [10, 9, 5, 5, 10, 4],
            'RClose': [10, 9, 5, 5, 10, 3],
            'V1': range(6),
            'V2': range(1, 7),
            'TestFlag': [False] * 4 + [True] * 2
        })
        self.data['Date'] = convert_date_array(self.data.Date)
        self.data2 = self.data.copy()
        self.data2.Date = ['2015-02-01', '2015-02-02', '2015-02-03',
                           '2015-02-04', '2015-02-05', '2015-02-06']
        self.data2['Date'] = convert_date_array(self.data2.Date)
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04',
                 '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08']
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 8
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['MA5_AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0, 0, 0]
        data['TestFlag'] = [True] * 8
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        data = data.append(data2).reset_index(drop=True)
        data['AdjVwap'] = data['AdjClose'].copy()
        self.data3 = data

    def test_make_responses(self):
        dc = DataContainerPairs()
        dc._make_responses(self.data)
        dc._processed_train_responses
        dc._response_arg_map

    def test_append_ern_date_blackout(self):
        result = append_ern_date_blackout(self.data3, -1, 1)
        benchmark = np.array([0, 1, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)
        result = append_ern_date_blackout(self.data3, 0, 1)
        benchmark = np.array([0, 0, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)
        result = append_ern_date_blackout(self.data3, 0, 3)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 0, 0]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)

    def test_make_anchor_ret(self):
        data = self.data3.copy()
        data = append_ern_date_blackout(data, -1, 1)
        result = make_anchor_ret(data)

    def test_make_ern_return(self):
        result = make_ern_return(self.data3)
        benchmark = self.data3[['SecCode', 'Date']].copy()
        benchmark['EARNINGS_Ret'] = [0.] * 4 + [1.] * 4 + [0.] * 4 + [1.] * 4
        assert_frame_equal(result, benchmark)

    def test_make_ibes_increases_decreases(self):
        data = pd.DataFrame()
        data['SecCode'] = ['a'] * 9
        data['Date'] = ['2015-01-01', '2015-01-02', '2015-01-03',
                        '2015-01-04', '2015-01-05', '2015-01-06',
                        '2015-01-07', '2015-01-08', '2015-01-09']
        data['PTARGETMEAN'] = [1, 1, 2, 2, 2, 2, 2, 2, 3]
        result = make_ibes_increases_decreases(data)
        benchmark = data[['SecCode', 'Date']].copy()
        benchmark['IBES_Target_Increase'] = [0, 0, 1, .8, .6, .4, .2, 0, 1]
        benchmark['IBES_Target_Decrease'] = [0.] * 9
        assert_frame_equal(result, benchmark)

    def test_make_ibes_discount(self):
        data = pd.DataFrame()
        data['SecCode'] = ['a'] * 9
        data['Date'] = ['2015-01-01', '2015-01-02', '2015-01-03',
                        '2015-01-04', '2015-01-05', '2015-01-06',
                        '2015-01-07', '2015-01-08', '2015-01-09']
        data['PTARGETUNADJ'] = [11, 11, 12, 12, 12, 12, 12, 12, 13]
        data['RClose'] = [10] * 9
        result = make_ibes_discount(data)
        benchmark = data[['SecCode', 'Date']].copy()
        benchmark['IBES_Discount'] = [0.1, 0.1, 0.2, 0.2, 0.2,
                                      0.2, 0.2, 0.2, 0.3]
        benchmark['IBES_Discount_Smooth'] = [np.nan, np.nan, np.nan, 0.15,
                                             0.175, 0.2, 0.2, 0.2, 0.225]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
