import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.utils import ern_date_blackout
from ram.strategy.starmine.utils import make_anchor_ret_rank
from ram.strategy.starmine.utils import ern_return
from ram.strategy.starmine.utils import smoothed_responses

from gearbox import create_time_index


class DataContainer1(object):

    def __init__(self):
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()

    def get_args(self):
        return {
            'response_days': [[2, 4], [2], [3]],
            'training_qtrs': [-99]
        }

    def prep_data(self, response_days, training_qtrs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        # Fresh copies of processed raw data
        train_data, test_data, features = self._get_train_test_features()
        # Adjust per hyperparameters
        train_data = self._trim_training_data(train_data, training_qtrs)
        train_data = self._add_response_variables(train_data, response_days)

        self.train_data = train_data
        self.test_data = test_data
        self.features = features

    def add_data(self, data):
        """
        Takes in raw data, processes it and caches it
        """
        features = [
            'ARM', 'ARMREVENUE', 'ARMRECS', 'ARMEARNINGS', 'ARMEXRECS',
            'EPSESTIMATE',
            
            #'EPSSURPRISE', 'EBITDAESTIMATE', 'EBITDASURPRISE',
            #'REVENUEESTIMATE', 'REVENUESURPRISE', 'SESPLITFACTOR',
            
            #'SIRANK', 'SIMARKETCAPRANK', 'SISECTORRANK',
            #'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',
            
            # Additional Pricing Cols
            'PRMA120_AvgDolVol', 'PRMA10_AdjClose', 'PRMA20_AdjClose',
            'BOLL10_AdjClose', 'BOLL20_AdjClose', 'BOLL60_AdjClose',
            'MFI10_AdjClose', 'MFI20_AdjClose', 'MFI60_AdjClose',
            'RSI10_AdjClose', 'RSI20_AdjClose', 'RSI60_AdjClose',
            'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
            'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose'
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

        # NEW FEATURES
        # Blackout flags and anchor returns
        data = ern_date_blackout(data, offset1=-1, offset2=1)

        data = make_anchor_ret_rank(data, init_offset=3, window=10)

        data = ern_return(data)

        # Rank and create binaries for extreme values
        features = features + ['blackout', 'anchor_ret_rank', 'earnings_ret']

        keep_inds = data[features].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]

        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]
        self.features = features

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

    def _add_response_variables(self, data, response_days, response_thresh=.3):
        return data.merge(smoothed_responses(data, days=response_days,
                                             thresh=response_thresh))
