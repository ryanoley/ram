import numpy as np
import pandas as pd
import datetime as dt

from gearbox import create_time_index
from ram.strategy.starmine.data.features import *
from ram.strategy.basic.utils import make_variable_dict

SPY_PATH = os.path.join(os.getenv('DATA'), 'ram', 'prepped_data',
                        'PostErnStrategy','spy.csv')


class DataContainer1(object):

    def __init__(self):
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._processed_pricing_data = pd.DataFrame()
        self._set_features()
        self.prep_market_dicts()

    def get_args(self):
        return {
            'response_days': [20, 30],
            'training_qtrs': [-99]
        }

    def prep_market_dicts(self):
        market_data = read_spy_data()
        market_data['SplitMultiplier'] = market_data.SplitFactor.pct_change().fillna(0) + 1
        market_dict = {}
                # Process implementation details
        market_dict['close'] = make_variable_dict(market_data, 'RClose')
        market_dict['vwap'] = make_variable_dict(market_data, 'RVwap')
        market_dict['dividend'] = make_variable_dict(market_data,
                                                     'RCashDividend', 0)
        market_dict['split_mult'] = make_variable_dict(market_data,
                                                       'SplitMultiplier', 1)
        self.market_dict = market_dict
        self._market_data = market_data

    def add_data(self, data, entry_day=2):
        """
        Takes in raw data, processes it and caches it
        """

        data = self.process_raw_data(data)

        # Filter nans
        keep_inds = data[self.features].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]
        keep_inds = data[self.ret_cols].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]

        # Get data for daily pl calculations
        pricing_data = data[data.TestFlag].copy()
        pricing_data['RCashDividend'] = 0.
        pricing_data['LiveFlag'] = 0
        pricing_cols = ['Date', 'SecCode', 'RClose', 'RVwap', 'EARNINGSFLAG', 
                        'RCashDividend', 'SplitMultiplier', 'AvgDolVol',
                        'MarketCap', 'LiveFlag']

        # Filter train and test data to one entry date
        self._entry_day = entry_day
        data = self.get_data_subset(data, entry_day - 1)

        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]
        self._processed_pl_data = pricing_data[pricing_cols]

    def process_raw_data(self, data):
        
        # ~~~~~~ CLEAN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        data.AdjVwap = np.where(
            data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
        data.AdjClose = np.where(
            data.AdjClose.isnull(), data.AdjVwap, data.AdjClose)

        # SPLITS
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) + 1

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

        # Several different returns
        data = get_vwap_returns(data, 20, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 30, hedged=True,
                                market_data=self._market_data)

        return data

    def prep_data(self, response_days, training_qtrs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        # Fresh copies of processed raw data
        train_data, test_data, daily_pl = self._get_train_test_daily_data()
        # Adjust per hyperparameters
        train_data = self._trim_training_data(train_data, training_qtrs)
        train_data = self._add_response_variables(train_data, response_days)
        daily_pl = self._add_exit_flag(daily_pl, response_days)

        self.train_data = train_data
        self.test_data = test_data
        self.daily_pl_data = daily_pl
    
        # Process implementation details
        self.close_dict = make_variable_dict(
            self._processed_pl_data, 'RClose')
        self.vwap_dict = make_variable_dict(
            self._processed_pl_data, 'RVwap')
        self.dividend_dict = make_variable_dict(
            self._processed_pl_data, 'RCashDividend', 0)
        self.split_mult_dict = make_variable_dict(
            self._processed_pl_data, 'SplitMultiplier', 1)
        self.liquidity_dict = make_variable_dict(
            self._processed_pl_data, 'AvgDolVol')
        self.market_cap_dict = make_variable_dict(
            self._processed_pl_data, 'MarketCap')

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
        self.ret_cols = ['Ret20', 'Ret30']
        return

    ###########################################################################

    def _get_train_test_daily_data(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy(),
            self._processed_pl_data.copy(),
        )

    def _trim_training_data(self, data, training_qtrs):
        if training_qtrs == -99:
            return data
        inds = create_time_index(data.Date)
        max_ind = np.max(inds)
        return data.iloc[inds > (max_ind-training_qtrs)]

    def get_data_subset(self, data, ern_flag_offset):
        '''
        Get a subset of the data based on an int offset from EARNINGSFLAG
        '''
        assert 'EARNINGSFLAG' in data.columns
        ernflag = data.pivot(index='Date', columns='SecCode',
                             values='EARNINGSFLAG')
        ernflag = ernflag.shift(ern_flag_offset).fillna(0)
        ernflag = ernflag.unstack().reset_index()
        ernflag.columns = ['SecCode', 'Date', 'DtSelect']
        ernflag = ernflag[ernflag.DtSelect == 1].drop('DtSelect', axis=1)
        subset = pd.merge(data, ernflag)
        return subset

    def _add_response_variables(self, data, response_days):
        return data.merge(fixed_response(data, days=response_days))

    def _add_exit_flag(self, data, response_days):
        assert 'EARNINGSFLAG' in data.columns
        ernflag = data.pivot(index='Date', columns='SecCode',
                             values='EARNINGSFLAG')
        ernflag = ernflag.shift(response_days + self._entry_day).fillna(0)     
        ernflag.iloc[-1] = 1
        ernflag = ernflag.unstack().reset_index()
        ernflag.columns = ['SecCode', 'Date', 'ExitFlag']
        data = pd.merge(data, ernflag, how='left')
        return data



def read_spy_data(spy_path=None):
    if spy_path is None:
        spy_path = SPY_PATH
    spy = pd.read_csv(spy_path)
    spy.Date = convert_date_array(spy.Date)
    return spy
