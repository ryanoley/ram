import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.statarb.version_004.data.data_container import *


class TestDataContainer(unittest.TestCase):

    def setUp(self):
        pass

    def test_set_args(self):
        data = pd.DataFrame({
            'SecCode': ['AAPL', 'TSLA'] * 5,
            'Date': range(10),
            'TimeIndex': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
            'Response_Simple_2': 0
        })
        container = DataContainer()
        container._processed_train_data = data.copy()
        container._processed_test_data = data.copy()
        container.set_args(response_days=2,
                           response_type='Simple')
        self.assertListEqual(range(8), container._train_data.index.tolist())
        self.assertListEqual(
            range(8), container._train_data_responses.index.tolist())

    def test_get_train_test(self):
        data = pd.DataFrame({
            'SecCode': ['AAPL', 'TSLA'] * 3,
            'Date': range(6),
            'TimeIndex': [1, 1, 2, 2, 3, 3],
        })
        container = DataContainer()
        container._processed_train_data = data.copy()
        container._processed_test_data = data.copy()
        result = container._get_train_test()
        assert_frame_equal(result[0], data)

    def test_prep_live_data(self):
        features = accounting_features + starmine_features
        data = pd.DataFrame(columns=['SecCode', 'Date'] + features)
        data['SecCode'] = ['AAPL', 'TSLA', 'BAC']
        data['Date'] = '2010-01-01'
        data[features] = 10
        data['RClose'] = 100
        data['AvgDolVol'] = 100
        data2 = data.copy()
        data2['Date'] = '2010-01-02'
        data2[features] = 20
        data = data.append(data2).reset_index(drop=True)
        market_data = pd.DataFrame()
        market_data['SecCode'] = ['50311', '11113']
        market_data['Date'] = 1
        market_data['AdjClose'] = 1
        container = DataContainer()
        container.prep_live_data(data, market_data)
        self.assertTrue('raw_data' in container._live_prepped_data)
        self.assertTrue('market_data' in container._live_prepped_data)
        self.assertTrue('prepped_data' in container._live_prepped_data)
        self.assertTrue('prepped_features' in container._live_prepped_data)
        self.assertTrue(hasattr(container, '_constructor_data'))

    def test_process_live_data(self):
        # Training data file
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=i) for i in range(60)]
        dates2 = dates * 3
        dates2.sort()
        data = pd.DataFrame()
        data['SecCode'] = ['AAPL', 'TSLA', 'BAC'] * 60
        data['Date'] = dates2
        data['SplitFactor'] = 1
        data['AdjOpen'] = range(180)
        data['AdjHigh'] = range(180)
        data['AdjLow'] = range(180)
        data['AdjClose'] = range(180)
        data['RClose'] = range(180)
        data['AdjVwap'] = range(180)
        data['AdjVolume'] = range(180)
        data['AvgDolVol'] = range(180)
        features = accounting_features + starmine_features
        for f in features:
            data[f] = np.random.randn(180)
        dates2 = dates * 2
        dates2.sort()
        market_data = pd.DataFrame()
        market_data['SecCode'] = ['50311', '11113'] * 60
        market_data['Date'] = dates2
        market_data['AdjClose'] = np.random.randn(120)
        container = DataContainer()
        container.prep_live_data(data, market_data)
        columns = ['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose',
                   'AdjVolume', 'AdjVwap', 'RClose']
        live_pricing = pd.DataFrame(columns=['SecCode', 'Ticker'] + columns)
        live_pricing['SecCode'] = ['1', '2', '3', '50311', '11113']
        live_pricing['Ticker'] = ['AAPL', 'TSLA', 'GOOG', 'SPY', 'VXX']
        live_pricing[columns] = 10
        container.process_live_data(live_pricing)
        result = container._processed_test_data
        self.assertEqual(len(result), 3)
        self.assertEqual(result.Date.iloc[0], 0)
        import pdb; pdb.set_trace()
        x = 10

    def test_calculate_avgdolvol(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 3 + ['B'] * 3
        data['Date'] = [1, 2, 3] * 2
        data['AdjVwap'] = [10, 20, 30] * 2
        data['AdjVolume'] = 100
        result = calculate_avgdolvol(data, 2)
        benchmark = np.array([np.nan, 1500.0, 2500.0, np.nan, 1500.0, 2500.0])
        assert_array_equal(result.AvgDolVol.values, benchmark)

    def Xtest_process_training_data(self):
        container = DataContainer()

    def Xtest_make_responses(self):
        container = DataContainer()

    def test_make_features_live(self):
        features = accounting_features + starmine_features
        data = pd.DataFrame(columns=['SecCode', 'Date'] + features)
        data['SecCode'] = ['AAPL', 'TSLA', 'BAC']
        data['Date'] = '2010-01-01'
        data.loc[0, features] = 10
        data.loc[1, features] = 30
        data.loc[2, features] = 20
        data2 = data.copy()
        data2['Date'] = '2010-01-02'
        data = data.append(data2).reset_index(drop=True)
        container = DataContainer()
        result = container._make_features(data, live_flag=True)
        self.assertEqual(result[0].Date.unique()[0], dt.date.today())
        features.sort()
        self.assertListEqual(features, result[1])
        means = pd.DataFrame()
        means['SecCode'] = result[0]['SecCode']
        means['Result'] = result[0][features].mean(axis=1)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['AAPL', 'TSLA', 'BAC']
        benchmark['Result'] = [1/3., 3/3., 2/3.]
        assert_frame_equal(means, benchmark)

    def test_make_technical_features_live(self):
        # 60 days of dates
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=i) for i in range(60)]
        dates = dates * 3
        dates.sort()
        data = pd.DataFrame()
        data['SecCode'] = ['AAPL', 'TSLA', 'BAC'] * 60
        data['Date'] = dates
        data['AdjOpen'] = range(1, 181)
        data['AdjHigh'] = range(1, 181)
        data['AdjLow'] = range(1, 181)
        data['AdjClose'] = range(1, 181)
        data['AdjVolume'] = range(1, 181)
        data['AvgDolVol'] = range(1, 181)
        data['keep_inds'] = True
        container = DataContainer()
        result = container._make_technical_features(data, live_flag=True)
        self.assertListEqual(result[0].SecCode.tolist(),
                             ['AAPL', 'TSLA', 'BAC'])
        self.assertEqual(result[0].Date.iloc[0], dt.date.today())
        self.assertIsInstance(result[1], list)

    def Xtest_initial_clean(self):
        pass

    def test_trim_to_one_month(self):
        container = DataContainer()
        data = pd.DataFrame()
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 2, 1),
                        dt.date(2010, 3, 1), dt.date(2010, 4, 1)]
        data['TestFlag'] = [False, False, True, True]
        result = container._trim_to_one_month(data)
        benchmark = data.iloc[1:]
        assert_frame_equal(result, benchmark)

    def test_create_split_multiplier(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 4 + ['B'] * 4
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 2, 1),
                        dt.date(2010, 3, 1), dt.date(2010, 4, 1)] * 2
        data['SplitFactor'] = [1, 1.5, 1.5, 1.5, 1, 1, 1, 2]
        result = create_split_multiplier(data)
        benchmark = [1, 1.5, 1.0, 1.0, 1.0, 1.0, 1.0, 2]
        result.SplitMultiplier.tolist()
        self.assertListEqual(result.SplitMultiplier.tolist(), benchmark)

    def test_merge_live_pricing_data(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 2 + ['B'] * 2 + ['C'] * 2
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 3
        data['TestFlag'] = True
        data['Close'] = range(6)
        data['SomeOther'] = range(6)
        live_data = pd.DataFrame()
        live_data['SecCode'] = ['A', 'B', 'C']
        live_data['Close'] = [10, 11, 12]
        result = merge_live_pricing_data(data, live_data)
        assert_frame_equal(result.iloc[:6].astype(str), data.astype(str))
        self.assertListEqual(result.SecCode.iloc[6:].tolist(), ['A', 'B', 'C'])
        self.assertEqual(result.Date.iloc[6], dt.date.today())
        self.assertListEqual(result.SomeOther.iloc[6:].tolist(), [np.nan] * 3)
        self.assertTupleEqual(result.shape, (9, 5))

    def test_merge_live_pricing_market_data(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 2 + ['B'] * 2 + ['C'] * 2
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 3
        data['AdjClose'] = range(6)
        live_data = pd.DataFrame()
        live_data['SecCode'] = ['A', 'B', 'C']
        live_data['AdjClose'] = [11, 12, 13]
        result = merge_live_pricing_market_data(data, live_data)
        self.assertListEqual(result.SecCode.iloc[6:].tolist(), ['A', 'B', 'C'])
        self.assertListEqual(result.AdjClose.iloc[6:].tolist(), [11, 12, 13])
        self.assertEqual(result.Date.iloc[6], dt.date.today())
        self.assertTupleEqual(result.shape, (9, 3))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
