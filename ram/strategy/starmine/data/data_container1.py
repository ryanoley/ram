import numpy as np
import pandas as pd
import datetime as dt

from gearbox import create_time_index, convert_date_array
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
        self._prep_market_pricing_dicts()

    def get_args(self):
        return {
            'response_days': [20, 30, 40],
            #'threshA': [.10, .25, .25], 
            'training_qtrs': [-99]
        }

    def _prep_market_pricing_dicts(self):
        market_data = read_spy_data()
        market_data['SplitMultiplier'] = market_data.SplitFactor.pct_change().fillna(0) + 1
        market_data.loc[:, 'SecCode'] = 'spy'

        self.mkt_vwap_dict = make_variable_dict(market_data, 'RVwap')
        self.mkt_adj_close_dict = make_variable_dict(market_data, 'AdjClose')
        self.mkt_close_dict = make_variable_dict(market_data, 'RClose')
        self.mkt_dividend_dict = make_variable_dict(market_data,
                                                'RCashDividend', 0)
        self.mkt_split_mult_dict = make_variable_dict(market_data,
                                                'SplitMultiplier', 1)
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
        pricing_cols = ['Date', 'SecCode', 'TM1', 'RClose', 'RVwap',
                        'EARNINGSFLAG',  'RCashDividend', 'SplitMultiplier',
                        'AvgDolVol']

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
        data.TM1 = convert_date_array(data.TM1)

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
        data.RevMissBeat.replace(np.inf, 0, inplace=True)
        data.RevMissBeat.replace(-np.inf, 0, inplace=True)

        # SMART ESTIMATE REVISIONS
        data = get_se_revisions(data, 'EPSESTIMATE', 'eps_est_change',
                                window=10)
        data = get_se_revisions(data, 'EBITDAESTIMATE', 'ebitda_est_change',
                                window=10)
        data = get_se_revisions(data, 'REVENUEESTIMATE', 'rev_est_change',
                                window=10)

        # Several different returns
        data = get_vwap_returns(data, 10, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 20, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 30, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 40, hedged=True,
                                market_data=self._market_data)

        # Accounting infs
        accounting_cols = ['NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',
        'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',
        'EBITGROWTHQ', 'EBITGROWTHTTM',
        'SALESGROWTHQ', 'SALESGROWTHTTM']
        data[accounting_cols] = data[accounting_cols].replace(np.inf, 0.)
        data[accounting_cols] = data[accounting_cols].replace(-np.inf, 0.)
        return data

    def prep_data(self, response_days, training_qtrs, **kwargs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        # Fresh copies of processed raw data
        train_data, test_data, daily_pl = self._get_train_test_daily_data()
        # Adjust per hyperparameters
        train_data = self._trim_training_data(train_data, training_qtrs)
        train_data = self._add_response_variables(train_data, response_days,
                                                  **kwargs)

        self.train_data = train_data
        self.test_data = test_data
        daily_pl.sort_values('Date', inplace=True)
        self.test_dates = daily_pl[['Date', 'TM1']].drop_duplicates()
        self.test_ids = np.sort(test_data.SecCode.unique())

        # Process implementation details
        self.close_dict = make_variable_dict(daily_pl, 'RClose')
        self.vwap_dict = make_variable_dict(daily_pl, 'RVwap')
        self.dividend_dict = make_variable_dict(daily_pl, 'RCashDividend', 0)
        self.split_mult_dict = make_variable_dict(daily_pl, 'SplitMultiplier',
                                                  1)
        self.liquidity_dict = make_variable_dict(daily_pl, 'AvgDolVol')
        self.exit_dict = self._make_exit_dict(daily_pl, response_days)

    def _set_features(self):
        features = [
        # Pricing and Vol vars
        'PRMA10_AdjClose', 'PRMA20_AdjClose', 'PRMA60_AdjClose',
        'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
        'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',

        'anchor_ret', 'anchor_ret_rank',

        # Earnings Return Variables
        'EARNINGSRETURN', 'PrevRet',
        'ern_ret_bin1', 'ern_ret_bin2', 'ern_ret_bin3',

        # Analyst Estimate change variables (Starmine)
        'eps_est_change', 'ebitda_est_change', 'rev_est_change',

        'RevMissBeat',
        
        # Accounting Variables
        'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',
        'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',
        'EBITGROWTHQ', 'EBITGROWTHTTM',
        'SALESGROWTHQ', 'SALESGROWTHTTM'
        ]
        self.features = features
        self.ret_cols = ['Ret10', 'Ret20', 'Ret30', 'Ret40']
        return

    ###########################################################################

    def _get_train_test_daily_data(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy(),
            self._processed_pl_data.copy(),
        )

    def get_pricing_dicts(self, date, mkt_prices=False):
        try:
            if mkt_prices:
                vwaps = self.mkt_vwap_dict[date]
                closes = self.mkt_close_dict[date]
                adj_closes = self.mkt_adj_close_dict[date]
                dividends = self.mkt_dividend_dict[date]
                splits = self.mkt_split_mult_dict[date]
                return vwaps, closes, adj_closes, dividends, splits
            else:
                vwaps = self.vwap_dict[date]
                closes = self.close_dict[date]
                dividends = self.dividend_dict[date]
                splits = self.split_mult_dict[date]
                return vwaps, closes, dividends, splits
        except KeyError:
            if mkt_prices:
                return {}, {}, {}, {}, {}
            else:
                return {}, {}, {}, {}

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

    def _make_exit_dict(self, data, response_days):
        assert 'EARNINGSFLAG' in data.columns
        if type(response_days) is list:
            response_days = max(response_days)
        ernflag = data.pivot(index='Date', columns='SecCode',
                             values='EARNINGSFLAG')
        ernflag = ernflag.shift(response_days + self._entry_day).fillna(0)     
        ernflag.iloc[-1] = 1
        ernflag = ernflag.unstack().reset_index()
        ernflag.columns = ['SecCode', 'Date', 'ExitFlag']

        ernflag = ernflag.loc[ernflag.ExitFlag == 1]
        exit_dict = {k: g["SecCode"].tolist() for k,g in ernflag.groupby("Date")}

        return exit_dict

    def _add_response_variables(self, data, response_days):
        return data.merge(fixed_response(data, days=response_days))
        #return data.merge(smoothed_responses(data, thresh=threshA,
        #                                     days=response_days))


def read_spy_data(spy_path=None):
    if spy_path is None:
        spy_path = SPY_PATH
    spy = pd.read_csv(spy_path)
    spy.Date = convert_date_array(spy.Date)
    return spy
