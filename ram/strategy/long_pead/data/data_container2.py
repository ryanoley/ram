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
from ram.strategy.long_pead.data.extract_alpha import read_all_extract_alpha_files
from ram.strategy.long_pead.data.extract_alpha import get_extract_alpha_data


from gearbox import create_time_index, convert_date_array


class DataContainer2(DataContainer1):
    """
    Extract Alpha implementation
    """
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
        # Trim only one quarter's worth of training data
        min_date = data.Date[data.TestFlag].min()
        trim_date = min_date - dt.timedelta(days=80)
        trim_date = dt.date(trim_date.year, trim_date.month, 1)
        data = data[data.Date >= trim_date].copy()

        # Separated for testing ease
        data, features = self._process_data(data)
        # Add market data
        if np.any(self._market_data):
            data = data.merge(self._market_data, how='left').fillna(0)
            features_mkt = self._market_data.columns.tolist()
            features_mkt.remove('Date')
            features += features_mkt

        if not hasattr(self, "_extract_alpha_data"):
            self._extract_alpha_data = read_all_extract_alpha_files()

        v1 = self._extract_alpha_data['tress']
        ea_data = _process_ea(
            get_extract_alpha_data(v1[0], v1[1], data), v1[1])
        data = data.merge(ea_data, how='left')
        ea_features = v1[1]

        v1 = self._extract_alpha_data['cam1']
        ea_data = _process_ea(
            get_extract_alpha_data(v1[0], v1[1], data), v1[1])
        data = data.merge(ea_data, how='left')
        ea_features.append(v1[1])

        v1 = self._extract_alpha_data['tm']
        ea_data = _process_ea(
            get_extract_alpha_data(v1[0], v1[1], data), v1[1])
        data = data.merge(ea_data, how='left')
        ea_features.append(v1[1])

        v1 = self._extract_alpha_data['dr']
        ea_data = _process_ea(
            get_extract_alpha_data(v1[0], v1[1], data), v1[1])
        data = data.merge(ea_data, how='left')
        ea_features.append(v1[1])

        # HACK
        ea_features2 = []
        for x in ea_features:
            if isinstance(x, list):
                ea_features2.extend(x)
            else:
                ea_features2.append(x)
        ea_features3 = ['EA_' + x for x in ea_features2]
        import pdb; pdb.set_trace()
        data = data.rename(columns={i: j for i, j in zip(ea_features2, ea_features3)})

        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]
        self.features = features + ea_features3
        self._time_index_data_for_responses[time_index] = \
            data[['SecCode', 'Date', 'AdjClose', 'TestFlag']]

    def _import_extract_alpha(self):
        pass


def _process_ea(data, features):
    data2 = outlier_rank(data, features[0])
    for f in features[1:]:
        data2 = data2.merge(outlier_rank(data, f))
    # Drop extremes
    drop_cols = [x for x in data2.columns if x.find('extreme') > 0]
    data2 = data2.drop(drop_cols, axis=1).fillna(0)
    return data2
