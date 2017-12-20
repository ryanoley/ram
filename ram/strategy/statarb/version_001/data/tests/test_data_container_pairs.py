import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_array_almost_equal
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
            'TimeIndex': 1,
            'AdjClose': [10, 9, 5, 5, 10, 4],
            'RClose': [10, 9, 5, 5, 10, 3],
            'V1': range(6),
            'V2': range(1, 7),
            'TestFlag': [False] * 4 + [True] * 2,
            'SplitFactor': 1,
            'GGROUP': '2020',
            'GSECTOR': '20'
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
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0, 0, 0]
        data['TestFlag'] = [True] * 8
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        data = data.append(data2).reset_index(drop=True)
        data['AdjVwap'] = data['AdjClose'].copy()
        self.data3 = data
        #
        data = self.data.copy()
        # Add additional variables to make _initial_clean work
        data['AdjOpen'] = data.AdjClose - 1
        data['AdjHigh'] = data.AdjClose + 2
        data['AdjLow'] = data.AdjClose - 2
        data['AdjVwap'] = data.AdjClose
        data['AdjVolume'] = 100
        data['AvgDolVol'] = 200
        data['PTARGETMEAN'] = 20
        data['PTARGETUNADJ'] = 10
        data['EARNINGSFLAG'] = 0
        for f in accounting_features:
            data[f] = 10
        for f in starmine_features:
            data[f] = 10
        data2 = data.copy()
        data2['SecCode'] = 'IBM'
        self.data4 = data.append(data2).reset_index(drop=True)
        data = pd.DataFrame({'SecCode': '50311', 'Date': dates,
                             'AdjClose': 10, 'PRMA10': 30})
        data['PRMA10_AdjClose'] = 10
        data['PRMA20_AdjClose'] = 10
        data['VOL10_AdjClose'] = 10
        data['VOL20_AdjClose'] = 10
        data['RSI10_AdjClose'] = 10
        data['RSI20_AdjClose'] = 10
        data['BOLL10_AdjClose'] = 10
        data['BOLL20_AdjClose'] = 10
        data2 = data.copy()
        data3 = data.copy()
        data4 = data.copy()
        data2['SecCode'] = '11132814'
        data3['SecCode'] = '11113'
        data4['SecCode'] = '10922530'
        self.market_data = data.append(data2).append(data3).append(data4)

    def test_prep_live_data(self):
        dc = DataContainerPairs()
        dc.prep_live_data(self.data4, self.market_data)

    def test_process_live_data(self):
        live_data = pd.DataFrame({
            'SecCode': ['AAPL', 'IBM', 'TSLA'],
            'Ticker': ['AAPL', 'IBM', 'TSLA'],
            'AdjOpen': [10, 20, 30],
            'AdjHigh': [10, 20, 30],
            'AdjLow': [10, 20, 30],
            'AdjClose': [10, 20, 30],
            'AdjVolume': [10, 20, 30]
        })
        dc = DataContainerPairs()
        dc.prep_live_data(self.data4, self.market_data)
        dc.process_live_data(live_data)

    def test_set_args(self):
        dc = DataContainerPairs()
        # Setup
        response_params = {'asdf': '1234'}
        dc._response_arg_map = {}
        dc._response_arg_map[str(response_params)] = 0
        dc._processed_train_responses = pd.DataFrame({
            'SecCode': ['A', 'B', 'C'] * 2,
            'Date': ['d'] * 6,
            'TimeIndex': [0, 0, 0, 1, 1, 1],
            0: [0, 1, 0] * 2
        })
        dc._processed_train_data = pd.DataFrame({
            'SecCode': ['A', 'B', 'C'] * 2,
            'Date': ['d'] * 6,
            'TimeIndex': [0, 0, 0, 1, 1, 1],
            'V1': range(6)
        })
        dc._processed_test_data = pd.DataFrame({
            'SecCode': ['A', 'B', 'C'] * 2,
            'Date': ['d'] * 6,
            'TimeIndex': [2] * 6,
            'V1': range(6)
        })
        dc.set_args(response_params, training_qtrs=1)
        dc.get_train_data()
        dc.get_train_responses()
        dc.get_test_data()

    def test_make_responses(self):
        dc = DataContainerPairs()
        dc._make_responses(self.data)
        result = dc._processed_train_responses.columns.tolist()
        params_count = len(dc.get_args()['response_params'])
        benchmark = ['SecCode', 'TimeIndex', 'Date'] + range(params_count)
        self.assertListEqual(result, benchmark)
        result = dc._response_arg_map.values()
        result.sort()
        self.assertListEqual(result, range(params_count))

    def test_make_ern_date_blackout(self):
        result = make_ern_date_blackout(self.data3, -1, 1)
        benchmark = np.array([0, 1, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)
        result = make_ern_date_blackout(self.data3, 0, 1)
        benchmark = np.array([0, 0, 1, 1, 0, 0, 0, 0]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)
        result = make_ern_date_blackout(self.data3, 0, 3)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 0, 0]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)
        # Adjust EARNINGSFLAG to end of period
        data4 = self.data3.copy()
        data4['EARNINGSFLAG'] = [0, 0, 0, 0, 0, 0, 1, 0] * 2
        result = make_ern_date_blackout(data4, 1, 3)
        benchmark = np.array([0, 0, 0, 0, 0, 0, 0, 1]*2)
        assert_array_equal(result.EARNINGS_Blackout.values, benchmark)

    def test_make_anchor_ret(self):
        data4 = self.data3.copy()
        data4['EARNINGSFLAG'] = [0, 1, 0, 0, 0, 0, 0, 0] * 2
        result = make_anchor_ret(data4, init_offset=1, window=2)
        benchmark = np.array([0, 0, 0, 1/3., 2/3., 0, 0, 0] * 2)
        assert_array_almost_equal(result.EARNINGS_AnchorRet.values, benchmark)

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
        benchmark = result[0].copy()
        benchmark['a'] = [0, 0, 1, .8, .6, .4, .2, 0, 1]
        assert_frame_equal(result[0], benchmark)
        benchmark['a'] = [0.0] * 9
        assert_frame_equal(result[1], benchmark)

    def test_make_ibes_discount(self):
        data = pd.DataFrame()
        data['SecCode'] = ['a'] * 9
        data['Date'] = ['2015-01-01', '2015-01-02', '2015-01-03',
                        '2015-01-04', '2015-01-05', '2015-01-06',
                        '2015-01-07', '2015-01-08', '2015-01-09']
        data['PTARGETUNADJ'] = [11, 11, 12, 12, 12, 12, 12, 12, 13]
        data['RClose'] = [10] * 9
        result = make_ibes_discount(data)
        benchmark = result[0].copy()
        benchmark['a'] = [np.nan, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]
        assert_frame_equal(result[0], benchmark)
        benchmark['a'] = [np.nan, np.nan, np.nan, np.nan, 0.15,
                          0.175, 0.2, 0.2, 0.2]
        assert_frame_equal(result[1], benchmark)

    def test_create_split_multiplier(self):
        result = create_split_multiplier(self.data2)
        self.assertTrue('SplitMultiplier' in result)
        self.assertTrue('SplitFactor' not in result)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
