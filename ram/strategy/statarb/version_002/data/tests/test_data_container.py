import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.statarb.version_002.data.data_container import *


class TestDataContainer(unittest.TestCase):

    def setUp(self):
        dates = ['2015-03-29', '2015-03-30', '2015-03-31',
                 '2015-04-01', '2015-04-02', '2015-04-03']
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 6,
            'Date': dates,
            'TimeIndex': 1,
            'AdjClose': [10, 9, 5, 5, 10, 4],
            'RClose': [10, 9, 5, 5, 10, 3],
            'AvgDolVol': [10] * 6,
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

        for f in accounting_features:
            data[f] = 10
        for f in starmine_features:
            data[f] = 10

        data2 = data.copy()

        data2['SecCode'] = 'IBM'
        for f in accounting_features:
            data2[f] *= 10
        for f in starmine_features:
            data2[f] *= 10

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
        dc = DataContainer()
        dc.prep_live_data(self.data4, self.market_data)
        self.assertTrue(hasattr(dc, '_live_prepped_data'))
        self.assertTrue(hasattr(dc, '_constructor_data'))
        result = dc._live_prepped_data['data_features_1']
        self.assertEqual(result.shape[0], 2)
        self.assertListEqual(result.Date.tolist(), [0, 0])

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
        dc = DataContainer()
        dc.prep_live_data(self.data4, self.market_data)
        dc.process_live_data(live_data)

    def test_merge_live_pricing_data(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A', 'A', 'B', 'B']
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 2
        data['TestFlag'] = [True, True, True, True]
        data['V1'] = [1, 2, 3, 4]
        data['V2'] = [3, 4, 5, 6]
        live_data = pd.DataFrame()
        live_data['SecCode'] = ['A', 'B']
        live_data['Ticker'] = ['A', 'B']
        live_data['V1'] = [10, 20]
        result = merge_live_pricing_data(data, live_data)
        self.assertEqual(result.shape[0], 6)

    def test_make_features(self):
        dc = DataContainer()
        result, rlist = dc._make_features(self.data4)
        rlist.sort()
        benchmark = accounting_features + starmine_features
        benchmark.sort()
        self.assertListEqual(rlist, benchmark)
        # Test data
        benchmark = pd.Series([0.75] + [0.5] * 5 + [0.75] + [1.0] * 5,
                              name='PE')
        assert_series_equal(result.PE, benchmark)

    def test_make_technical_features(self):
        dc = DataContainer()
        result, rlist = dc._make_technical_features(self.data4)
        # Most of the data doesn't process because there aren't
        # enough dates. Just simple test to draw attention to any changes
        self.assertEqual(len(rlist), 24)

    def test_set_args(self):
        dc = DataContainer()
        self.data4['Response_Simple_3'] = 0
        dc._processed_train_data = self.data4
        dc._processed_test_data = self.data4
        dc.set_args(response_days=3, response_type='Simple')
        # Setup
        benchmark = self.data4.Date.iloc[:3].tolist()
        benchmark = benchmark * 2
        self.assertListEqual(dc.train_data.Date.tolist(), benchmark)

    def test_make_responses(self):
        dc = DataContainer()
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04',
                 '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08',
                 '2015-01-09', '2015-01-10', '2015-01-11', '2015-01-12']
        data = pd.DataFrame()
        data['Date'] = convert_date_array((dates))
        data = data.append(data).append(data).append(
            data).reset_index(drop=True)
        data['SecCode'] = ['A'] * 12 + ['B'] * 12 + ['C'] * 12 + ['D'] * 12
        data['AdjClose'] = range(1, 13) + range(101, 113) + \
            range(1001, 1013) + range(10001, 10013)
        result = dc._make_responses(data)
        benchmark = ['SecCode', 'Date',
                     'Response_Simple_5', 'Response_Smoothed_5',
                     'Response_Simple_10', 'Response_Smoothed_10']
        self.assertListEqual(result.columns.tolist(), benchmark)
        self.assertEqual(result.Response_Simple_10.sum(), 6)

    def test_trim_to_one_month(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 8
        data['Date'] = convert_date_array([
            '2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04',
            '2015-02-01', '2015-02-02', '2015-02-03', '2015-02-04'])
        data['TestFlag'] = False
        dc = DataContainer()
        result = dc._trim_to_one_month(data)
        self.assertEqual(result.shape[0], 4)
        self.assertEqual(result.Date.iloc[0], dt.date(2015, 2, 1))

    def test_create_split_multiplier(self):
        result = create_split_multiplier(self.data2)
        self.assertTrue('SplitMultiplier' in result)
        self.assertTrue('SplitFactor' not in result)

    def test_make_technical_market_features(self):
        dc = DataContainer()
        dates = ['2015-01-{:02d}'.format(x) for x in range(1, 22)] * 2
        data = pd.DataFrame({
            'SecCode': ['AAA'] * 21 + ['BBB'] * 21,
            'Date': dates,
            'TimeIndex': 1,
            'AdjClose': range(11, 32) * 2,
        })
        data['Date'] = convert_date_array(data.Date)
        result = dc._make_technical_market_features(data, live_flag=False)
        benchmark = ['MKT_AdjClose_AAA', 'MKT_AdjClose_BBB',
                     'MKT_BOLL20_AAA', 'MKT_BOLL20_BBB', 'MKT_PRMA10_AAA',
                     'MKT_PRMA10_BBB', 'MKT_RANK_BOLL20_AAA',
                     'MKT_RANK_BOLL20_BBB', 'MKT_RANK_PRMA10_AAA',
                     'MKT_RANK_PRMA10_BBB', 'MKT_RANK_RSI10_AAA',
                     'MKT_RANK_RSI10_BBB', 'MKT_RANK_VOL10_AAA',
                     'MKT_RANK_VOL10_BBB', 'MKT_RSI10_AAA', 'MKT_RSI10_BBB',
                     'MKT_VOL10_AAA', 'MKT_VOL10_BBB']

        self.assertListEqual(result[1], benchmark)
        result = dc._make_technical_market_features(data, live_flag=True)
        self.assertListEqual(result[1], benchmark)
        self.assertEqual(result[0].shape[0], 1)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
