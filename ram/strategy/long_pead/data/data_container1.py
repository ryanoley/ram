import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import ern_date_blackout
from ram.strategy.long_pead.utils import make_anchor_ret_rank
from ram.strategy.long_pead.utils import ern_return
from ram.strategy.long_pead.utils import outlier_rank
from ram.strategy.long_pead.utils import smoothed_responses

from gearbox import create_time_index, convert_date_array


class DataContainer1(object):

    def __init__(self):
        self._import_market_data()
        self._time_index_data_for_responses = {}
        self._time_index_response_data = {}
        # Deliverable
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._features = []

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
        # Create data for downstream
        self.train_data = train_data
        self.test_data = test_data
        self.features = features

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

    def _process_data(self, data):
        """
        Separated from add data for testing purposes
        """
        features = [
            'PRMA120_AvgDolVol', 'PRMA10_AdjClose',
            'PRMA20_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
            'BOLL60_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose',
            'MFI60_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose',
            'RSI60_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose',
            'VOL60_AdjClose', 'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose',
            # Accounting Variables
            'NETINCOMEQ', 'NETINCOMETTM', 'SALESQ', 'SALESTTM', 'ASSETS',
            'CASHEV', 'FCFMARKETCAP',
            'NETINCOMEGROWTHQ',
            'NETINCOMEGROWTHTTM',
            'OPERATINGINCOMEGROWTHQ',
            'OPERATINGINCOMEGROWTHTTM',
            'EBITGROWTHQ',
            'EBITGROWTHTTM',
            'SALESGROWTHQ',
            'SALESGROWTHTTM',
            'FREECASHFLOWGROWTHQ',
            'FREECASHFLOWGROWTHTTM',
            'GROSSPROFASSET',
            'GROSSMARGINTTM',
            'EBITDAMARGIN',
            'PE',
            # StarMine variables
            'ARM', 'ARMREVENUE', 'ARMRECS', 'ARMEARNINGS',
            'ARMEXRECS', 'SIRANK', 'SIMARKETCAPRANK', 'SISECTORRANK',
            'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',
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
        data['GSECTOR'] = data.GGROUP.apply(lambda x: x[:2])

        # NEW FEATURES
        # Blackout flags and anchor returns
        data = ern_date_blackout(data, offset1=-2, offset2=4)

        data = make_anchor_ret_rank(data, init_offset=3, window=10)

        data = ern_return(data)

        # Handle nan values, rank and create binaries for extreme values
        data2 = outlier_rank(data, features[0])
        for f in features[1:]:
            data2 = data2.merge(outlier_rank(data, f))
        data = data.drop(features, axis=1)
        data = data.merge(data2)
        features = features + [f + '_extreme' for f in features] + \
            ['blackout', 'anchor_ret_rank', 'earnings_ret']
        return data, features

    ###########################################################################

    def _import_market_data(self):
        try:
            path = os.path.join(os.getenv('DATA'), 'ram',
                                'temp', 'market_data.csv')
            data = pd.read_csv(path, index_col=0)
            data['Date'] = convert_date_array(data.Date)

            spy_data = data[data.SecCode == 61494].drop('SecCode', axis=1)
            spy_data = spy_data.set_index('Date')
            spy_data.columns = ['{}_spy'.format(x) for x in spy_data.columns]

            vxx_data = data[data.SecCode == 10902726].drop('SecCode', axis=1)
            vxx_data = vxx_data.set_index('Date')
            vxx_data.columns = ['{}_vxx'.format(x) for x in vxx_data.columns]

            self._market_data = spy_data.join(
                vxx_data, how='outer').fillna(0).reset_index()
        except:
            self._market_data = None

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
        for t in time_indexes:
            responses = responses.append(resp_dict[t][data_name])
        return responses.reset_index(drop=True)
