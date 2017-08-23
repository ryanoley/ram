import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import ern_date_blackout
from ram.strategy.long_pead.utils import make_anchor_ret_rank
from ram.strategy.long_pead.utils import ern_return
from ram.strategy.long_pead.utils import outlier_rank
from ram.strategy.long_pead.utils import smoothed_responses
from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.data.data_container1 import DataContainer1

from gearbox import create_time_index, convert_date_array


class DataContainer2(DataContainer1):

    def get_args(self):
        return {
            'response_days': [[2, 4, 6], [2]],
            'response_thresh': [0.30],
            'training_qtrs': [-99, 12]
        }

    def prep_data(self, time_index,
                  response_days, response_thresh, training_qtrs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        # Get (and process if needed) response data
        response_data = self._get_response_data(time_index, response_days,
                                                response_thresh)
        # Fresh copies of processed raw data
        train_data, test_data, features = self._get_train_test_features()
        # Adjust per hyperparameters
        train_data = self._trim_training_data(train_data, training_qtrs)
        # Merge response data
        train_data = train_data.merge(response_data)

        # CREATE PAIRS?

        # Create data for downstream
        self.train_data = train_data
        self.test_data = test_data
        self.features = features

        # Process implementation details
        self.close_dict = make_variable_dict(
            test_data, 'RClose')
        self.dividend_dict = make_variable_dict(
            test_data, 'RCashDividend', 0)
        self.split_mult_dict = make_variable_dict(
            test_data, 'SplitMultiplier', 1)
        self.liquidity_dict = make_variable_dict(
            test_data, 'AvgDolVol')
        self.market_cap_dict = make_variable_dict(
            test_data, 'MarketCap')
        self.sector_dict = make_variable_dict(
            test_data, 'GSECTOR')

    def add_data(self, data, time_index):
        """
        Takes in raw data, processes it and caches it
        """
        # Separated for testing ease
        data, features = self._process_data(data)
        # Add market data
        if np.any(self._market_data):
            data = data.merge(self._market_data, how='left').fillna(0)
            features_mkt = self._market_data.columns.tolist()
            features_mkt.remove('Date')
            features += features_mkt
        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]
        self.features = features
        self._time_index_data_for_responses[time_index] = \
            data[['SecCode', 'Date', 'AdjClose', 'TestFlag']]
