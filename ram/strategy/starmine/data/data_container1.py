import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.data.features import *

from gearbox import create_time_index


class DataContainer1(object):

    def __init__(self):
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._set_features()

    def get_args(self):
        return {
            'response_days': [10, 20],
            'training_qtrs': [-99]
        }
    
    def add_data(self, data):
        """
        Takes in raw data, processes it and caches it
        """
        
        data = self.process_raw_data(data)

        # ~~~~~~ CLEAN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        data.AdjVwap = np.where(
            data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
        data.AdjClose = np.where(
            data.AdjClose.isnull(), data.AdjVwap, data.AdjClose)

        # SPLITS  - Is this needed?
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) + 1

        # Filter nans
        keep_inds = data[self.features].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]
        keep_inds = data[self.ret_cols].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]
        #data = data[data.EARNINGSFLAG == 0].reset_index(drop=True)
        data = self.get_data_subset(data, 3)

        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]

    
    def process_raw_data(self, data, entry_window=10):
        # Previous Earnings Return
        data = get_previous_ern_return(data, fillna = True,
                                       prior_data = self._processed_train_data)

        # Binary vars for previous wins
        data['ern_ret_bin1'] = (data.EARNINGSRETURN < -.03) & (data.PrevRet < -.03)
        data['ern_ret_bin2'] = (np.abs(data.EARNINGSRETURN) < .03) & (np.abs(data.PrevRet) < .03)
        data['ern_ret_bin3'] = (data.EARNINGSRETURN > .03) & (data.PrevRet > .03)

        # Achor Return
        data = ern_date_blackout(data, -1, 1)
        data = ern_price_anchor(data, 1, 15)
        data = make_anchor_ret_rank(data, 1, 15)

        # Revenue As compared to estimate
        data['RevMissBeat'] = ((data.REVENUEESTIMATEFQ1 - data.SALESQ) /
                                data.SALESQ).fillna(0.)

        # SMART ESTIMATE REVISIONS
        data = get_se_revisions(data, 'EPSESTIMATE', 'eps_est_change',
                                window=10)
        data = get_se_revisions(data, 'EBITDAESTIMATE', 'ebitda_est_change',
                                window=10)
        data = get_se_revisions(data, 'REVENUEESTIMATE', 'rev_est_change',
                                window=10)

        # Get subset of data 10 days after ern announcement
        data = filter_entry_window(data, window_len=10)

        # Several different returns
        data = get_vwap_returns(data, 5, hedged=True)
        data = get_vwap_returns(data, 10, hedged=True)
        data = get_vwap_returns(data, 20, hedged=True)

        return data

    def get_data_subset(self, data, t_entry):
        offset = t_entry - 1
        ernflag = data.pivot(index='Date', columns='SecCode', values='EARNINGSFLAG')
        ernflag = ernflag.shift(offset).fillna(0)
        ernflag = ernflag.unstack().reset_index()
        ernflag.columns = ['SecCode', 'Date', 'DtSelect']
        subset = pd.merge(data, ernflag)
        return subset

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

    
    def _set_features(self):
        features = [
        # Pricing and Vol vars
        'PRMA10_AdjClose', 'PRMA20_AdjClose', 'PRMA60_AdjClose',
        'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
        'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',

        'anchor_ret', 'anchor_ret_rank',

        # Earnings Return Variables
        'EARNINGSRETURN',
        'PrevRet', 'ern_ret_bin1', 'ern_ret_bin2', 'ern_ret_bin3',

        'eps_est_change', 'ebitda_est_change', 'rev_est_change'
        ]
        self.features = features
        self.ret_cols = ['Ret5', 'Ret10', 'Ret20']
        return


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

    def _add_response_variables(self, data, response_days):
        return data.merge(fixed_response(data, days=response_days))
