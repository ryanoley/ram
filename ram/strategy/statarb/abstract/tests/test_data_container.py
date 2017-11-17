import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.data.feature_creator import *
from ram.strategy.statarb.abstract.data_container import BaseDataContainer


class DataContainer(BaseDataContainer):

    def __init__(self):
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._processed_simulation_data = None

    def get_args(self):
        return {'v1': [1, 2]}

    def set_args(self, time_index, **kwargs):
        return None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_training_data(self):
        return self._processed_train_data

    def get_training_responses(self):
        return 0

    def get_training_feature_names(self):
        return ['a', 'b', 'c']

    def get_test_data(self):
        return self._processed_test_data

    def get_simulation_feature_dictionary(self):
        return {'pricing': self._processed_simulation_data,
                'pairs': pd.DataFrame()}

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def process_training_data(self, data, time_index):
        # Process some data
        pdata = data[['SecCode', 'Date', 'TestFlag']].merge(
            self._make_features(data))
        self._processed_train_data = \
            self._processed_train_data.append(pdata[~pdata.TestFlag])
        self._processed_test_data = pdata[pdata.TestFlag]
        sim_features = ['MarketCap', 'AvgDolVol', 'RClose',
                        'RCashDividend', 'SplitMultiplier']
        self._processed_simulation_data = data[data.TestFlag][sim_features]

    def process_training_market_data(self, add_market_data):
        return None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def prep_live_data(self, data):
        pass

    def process_live_data(self, data):
        # Match live data with data frame
        pdata = self._make_features(data, True)
        pdata.drop('Date', axis=1, inplace=True)
        return pdata

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_features(self, data, live_flag=False):
        open_ = clean_pivot_raw_data(data, 'AdjOpen')
        high = clean_pivot_raw_data(data, 'AdjHigh')
        low = clean_pivot_raw_data(data, 'AdjLow')
        close = clean_pivot_raw_data(data, 'AdjClose')
        volume = clean_pivot_raw_data(data, 'AdjVolume')
        # Set correct method for training or live implementation
        prma = PRMA(live_flag)
        # Create variables
        v1 = prma.fit(close, 2)
        v2 = prma.fit(close, 4)
        v3 = outlier_rank(v2)
        # Unstack and set training/test data
        pdata = unstack_label_data(v1, 'PRMA2')
        pdata = pdata.merge(unstack_label_data(v2, 'PRMA4'))
        pdata = pdata.merge(unstack_label_data(v3[0], 'PRMA4_Rank'))
        pdata = pdata.merge(unstack_label_data(v3[1], 'PRMA4_Extreme'))
        return pdata


class TestBaseDataContainer(unittest.TestCase):

    def setUp(self):
        pass

    def test_process_training_data(self):
        df = pd.DataFrame()
        df['SecCode'] = ['a'] * 6 + ['b'] * 6
        df['Date'] = [dt.date(2010, 1, i) for i in range(1, 7)] * 2
        df['TestFlag'] = [False, False, False, False, True, True] * 2
        df['AdjHigh'] = range(1, 13)
        df['AdjLow'] = range(1, 13)
        df['AdjClose'] = range(1, 13)
        df['AdjOpen'] = range(1, 13)
        df['AdjVolume'] = range(12)
        df['MarketCap'] = [100] * 6 + [200] * 6
        df['AvgDolVol'] = [4] * 6 + [2] * 6
        df['RClose'] = range(1, 13)
        df['RCashDividend'] = [0] * 12
        df['SplitMultiplier'] = [1] * 12
        data = DataContainer()
        data.process_training_data(df, 10)
        t = data._processed_train_data
        self.assertEqual(len(t), 8)
        self.assertTrue('SecCode' in t)
        self.assertTrue('Date' in t)
        self.assertTrue('TestFlag' in t)
        self.assertTrue('PRMA2' in t)
        self.assertTrue('PRMA4' in t)
        self.assertTrue('PRMA4_Rank' in t)
        self.assertTrue('PRMA4_Extreme' in t)

    def test_process_live_data(self):
        df = pd.DataFrame()
        df['SecCode'] = ['a'] * 6 + ['b'] * 6
        df['Date'] = [dt.date(2010, 1, i) for i in range(1, 7)] * 2
        df['TestFlag'] = [False, False, False, False, True, True] * 2
        df['AdjHigh'] = range(1, 13)
        df['AdjLow'] = range(1, 13)
        df['AdjClose'] = range(1, 13)
        df['AdjOpen'] = range(1, 13)
        df['AdjVolume'] = range(12)
        df['MarketCap'] = [100] * 6 + [200] * 6
        df['AvgDolVol'] = [4] * 6 + [2] * 6
        df['RClose'] = range(1, 13)
        df['RCashDividend'] = [0] * 12
        df['SplitMultiplier'] = [1] * 12
        data = DataContainer()
        data.process_live_data(df)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
