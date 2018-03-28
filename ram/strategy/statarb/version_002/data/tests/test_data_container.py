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

    def Xtest_prep_live_data(self):
        container = DataContainer()

    def Xtest_process_live_data(self):
        container = DataContainer()

    def Xtest_process_training_data(self):
        container = DataContainer()

    def Xtest_make_responses(self):
        container = DataContainer()

    def Xtest_make_features(self):
        container = DataContainer()

    def Xtest_make_technical_features(self):
        container = DataContainer()

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
        live_data['Close'] = [1, 2, 3]
        result = merge_live_pricing_data(data, live_data)
        self.assertTupleEqual(result.shape, (9, 5))

    def test_merge_live_pricing_market_data(self):
        data = pd.DataFrame()
        data['SecCode'] = ['A'] * 2 + ['B'] * 2 + ['C'] * 2
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 3
        data['AdjClose'] = range(6)
        live_data = pd.DataFrame()
        live_data['SecCode'] = ['A', 'B', 'C']
        live_data['AdjClose'] = [1, 2, 3]
        result = merge_live_pricing_market_data(data, live_data)
        self.assertTupleEqual(result.shape, (9, 3))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
