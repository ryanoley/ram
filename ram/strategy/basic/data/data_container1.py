import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.basic.utils import make_variable_dict
from ram.strategy.basic.utils import outlier_rank
from ram.strategy.basic.utils import smoothed_responses

from gearbox import create_time_index, convert_date_array


class DataContainer1(object):

    def __init__(self):
        self._time_index_data_for_responses = {}
        self._time_index_response_data = {}
        # Deliverable
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._features = []

    def get_args(self):
        return {
            'response_days': [10],
            'response_thresh': [0.30],
            'training_qtrs': [-99]
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
        test_data = test_data.merge(response_data)
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

    def add_data(self, data, time_index):
        """
        Takes in raw data, processes it and caches it
        """
        # Separated for testing ease
        data, features = self._process_data(data)
        # Add market data
        if hasattr(self, '_market_data') and np.any(self._market_data):
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

    def _process_data(self, data):
        """
        Separated from add data for testing purposes
        """
        features = [
            'PRMA10_AdjClose'
        ]

        # ~~~~~~ CLEAN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        data.AdjVwap = np.where(
            data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
        data.AdjClose = np.where(
            data.AdjClose.isnull(), data.AdjVwap, data.AdjClose)

        # SPLITS: Instead of using the levels, use the CHANGE in levels.
        # This is necessary for the updating of positions and prices downstream
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1

        # Process gics and Sector
        data['GGROUP'] = data.GGROUP.fillna(0).astype(int).astype(str)

        # Handle nan values, rank and create binaries for extreme values
        data2 = outlier_rank(data, features[0])
        for f in features[1:]:
            data2 = data2.merge(outlier_rank(data, f))
        data = data.drop(features, axis=1)
        data = data.merge(data2)
        return data, features

    def add_market_data(self, data):
        if hasattr(self, '_market_data'):
            return

        name_map = pd.DataFrame([
            ('11132814', 'ShortVIX'),
            ('11113', 'VIX'),
            ('10922530', 'LongVIX'),

            ('11097', 'R1000Index'),
            ('11099', 'R1000Growth'),
            ('11100', 'R1000Value'),

            ('10955',  'R2000Index'),
            ('11101', 'R2000Growth'),
            ('11102', 'R2000Value'),

            ('11096', 'R3000Index'),
            ('11103', 'R3000Growth'),
            ('11104', 'R3000Value'),

            ('50311', 'SP500Index'),
            ('61258', 'SP500Growth'),
            ('61259', 'SP500Value'),
        ], columns=['SecCode', 'IndexName'])
        data = data.merge(name_map)

        features = [
            'AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose', 
            'VOL10_AdjClose', 'VOL20_AdjClose', 'RSI10_AdjClose',
            'RSI20_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose'
        ]

        market_data = pd.DataFrame()
        for f in features:
            pdata = data.pivot(index='Date', columns='IndexName', values=f)
            # Only keep levels (AdjClose) for VIX indexes
            if f == 'AdjClose':
                pdata = pdata[['ShortVIX', 'VIX', 'LongVIX']]
            pdata.columns = ['Mkt_{}_{}'.format(col, f.replace('_AdjClose', ''))
                             for col in pdata.columns]
            market_data = market_data.join(pdata, how='outer')
        # Nan Values set to medians of rows
        market_data = market_data.fillna(market_data.median())
        self._market_data = market_data.reset_index()

    ###########################################################################

    def _get_train_test_features(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy(),
            self.features
        )

    def _trim_training_data(self, data, training_qtrs):
        if training_qtrs == -99:
            return data
        inds = create_time_index(data.Date)
        max_ind = np.max(inds)
        return data.iloc[inds > (max_ind-training_qtrs)]

    def _get_response_data(self, time_index, response_days, response_thresh):
        # Process input
        if not isinstance(response_days, list):
            response_days = [response_days]
        resp_dict = self._time_index_response_data
        # Cached data exist for this particular quarter?
        if time_index not in resp_dict:
            resp_dict[time_index] = {}
        data_name = str((response_days, response_thresh))
        if data_name not in resp_dict[time_index]:
            resp_dict[time_index][data_name] = smoothed_responses(
                self._time_index_data_for_responses[time_index],
                days=response_days, thresh=response_thresh)
        # Stack time indexes
        time_indexes = resp_dict.keys()
        time_indexes.sort()
        responses = pd.DataFrame()
        for t in time_indexes[:-1]:
            temp_r = resp_dict[t][data_name]
            responses = responses.append(temp_r[~temp_r.TestFlag])
        t = time_indexes[-1]
        responses = responses.append(resp_dict[t][data_name])
        return responses.reset_index(drop=True)