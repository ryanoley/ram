import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.data.features import *
from ram.strategy.basic.utils import make_variable_dict
from gearbox import create_time_index, convert_date_array

SPY_PATH = os.path.join(os.getenv('DATA'), 'ram', 'prepped_data',
                        'PostErnStrategy', 'spy.csv')


class DataContainer1(object):

    def __init__(self):
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._processed_pricing_data = pd.DataFrame()
        self._set_features()
        self._set_market_pricing_dicts()

    def get_args(self):
        return {
            'response_days': [20],
            'training_qtrs': [-99]
        }

    def _set_market_pricing_dicts(self):
        spy = read_spy_data()
        spy.loc[:, 'SecCode'] = 'spy'
        spy['SplitMultiplier'] = spy.SplitFactor.pct_change().fillna(0) + 1

        self.mkt_dividend_dict = make_variable_dict(spy, 'RCashDividend', 0)
        self.mkt_split_mult_dict = make_variable_dict(spy, 'SplitMultiplier', 1)
        self.mkt_vwap_dict = make_variable_dict(spy, 'RVwap')
        self.mkt_adj_close_dict = make_variable_dict(spy, 'AdjClose')
        self.mkt_close_dict = make_variable_dict(spy, 'RClose')

        self._market_data = spy

    def add_data(self, data, entry_window=5):
        """
        Takes in raw data, processes it and caches it
        """

        data = self.process_raw_data(data)

        # Filter nans
        keep_inds = data[self.features].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]
        keep_inds = data[self.ret_cols].isnull().sum(axis=1) == 0
        data = data.loc[keep_inds]
        data = data[data.GGROUP.notnull()]

        # Get data for daily pl calculations
        pricing_data = data[data.TestFlag].copy()
        pricing_cols = ['Date', 'SecCode', 'GGROUP', 'TM1', 'RClose', 'RVwap',
                        'EARNINGSFLAG',  'RCashDividend', 'SplitMultiplier',
                        'AvgDolVol']

        # Filter train and test data to entry window
        data = self._filter_entry_window(data, entry_window)
        self._entry_window = entry_window

        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]
        self._processed_pl_data = pricing_data[pricing_cols]

    def process_raw_data(self, data):

        # SPLITS
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) + 1

        # Previous Earnings Return
        data = get_previous_ern_return(data,
                                       prior_data = self._processed_train_data)
        data.PrevRet.fillna(0., inplace=True)

        # Binary vars for previous wins
        data['ern_ret_bin1'] = ((data.EARNINGSRETURN < -.05) &
                                    (data.PrevRet < -.05))
        data['ern_ret_bin2'] = ((data.EARNINGSRETURN > .05) &
                                    (data.PrevRet > .05))

        # Achor Return
        data = ern_date_blackout(data, -1, 1)
        data = ern_price_anchor(data, 1, 15)
        data = make_anchor_ret_rank(data, 1, 15)

        # Revenue As compared to estimate
        data['RevMissBeat'] = ((data.REVENUEESTIMATEFQ1 - data.SALESQ) /
                                data.SALESQ).fillna(0.)

        # Smart Estimate Revisions
        data = get_cum_delta(data, 'EPSESTIMATE', 'eps_est_change',
                                 smart_est_column=True)
        data = get_cum_delta(data, 'EBITDAESTIMATE', 'ebitda_est_change',
                                 smart_est_column=True)
        data = get_cum_delta(data, 'REVENUEESTIMATE', 'rev_est_change',
                                 smart_est_column=True)

        data.eps_est_change /= np.abs(data.EPSESTIMATEFQ1)
        data.ebitda_est_change /= np.abs(data.EBITDAESTIMATEFQ1)
        data.rev_est_change /= np.abs(data.REVENUEESTIMATEFQ1)
        
        data.eps_est_change.fillna(0., inplace=True)
        data.rev_est_change.fillna(0., inplace=True)
        data.ebitda_est_change.fillna(0., inplace=True)

        # Price Target and Analyst Recommendations
        data = get_cum_delta(data, 'PTARGETUNADJ', 'prtgt_est_change')
        data['prtgt_est_change'] /= data.PTARGETUNADJ

        data['prtgt_discount'] = (data.RClose / data.PTARGETUNADJ) - 1
        data = get_cum_delta(data, 'prtgt_discount', 'prtgt_disc_change')

        data = get_cum_delta(data, 'RECMEAN', 'anr_rec_change')

        # Add multiple Returns for model training
        data = get_vwap_returns(data, 10, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 20, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 30, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 40, hedged=True,
                                market_data=self._market_data)

        # ~~~~~~ Clean and Filter ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        data.AdjVwap = np.where(data.AdjVwap.isnull(), data.AdjClose,
                                data.AdjVwap)
        data.AdjClose = np.where(data.AdjClose.isnull(), data.AdjVwap,
                                 data.AdjClose)
        data.TM1 = convert_date_array(data.TM1)

        # Replace infs
        inf_cols = [
            'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM', 'OPERATINGINCOMEGROWTHQ',
            'OPERATINGINCOMEGROWTHTTM', 'EBITGROWTHQ', 'EBITGROWTHTTM',
            'SALESGROWTHQ', 'SALESGROWTHTTM',
            'RevMissBeat', 'eps_est_change', 'ebitda_est_change',
            'rev_est_change', 'prtgt_est_change', 'prtgt_discount'
            ]
        data[inf_cols] = data[inf_cols].replace(np.inf, 0.)
        data[inf_cols] = data[inf_cols].replace(-np.inf, 0.)

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
        self.ind_groups = self._make_group_dict(daily_pl)
        

    def _set_features(self):
        features = [
        # Pricing and Vol vars
        'PRMA10_AdjClose', 'PRMA20_AdjClose', 'PRMA60_AdjClose',
        'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
        'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',

        'anchor_ret', 'anchor_ret_rank',

        # Earnings Return Variables
        'EARNINGSRETURN', 'PrevRet', 'ern_ret_bin1', 'ern_ret_bin2',

        # Analyst Estimate change variables (Starmine)
        'eps_est_change', 'ebitda_est_change', 'rev_est_change',
        'RevMissBeat',

        # Accounting Variables
        'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',
        'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',
        'EBITGROWTHQ', 'EBITGROWTHTTM',
        'SALESGROWTHQ', 'SALESGROWTHTTM',

        # Price target changes
        'prtgt_est_change', 'prtgt_discount', 'prtgt_disc_change',
        'anr_rec_change', 'RECMEAN'
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

    def _filter_entry_window(self, data, entry_window=10):
        assert 'EARNINGSFLAG' in data.columns
        ernflag = data.pivot(index='Date', columns='SecCode',
                             values='EARNINGSFLAG')
        entry_dates = ernflag.rolling(entry_window, min_periods=1).sum()
        entry_offset = entry_dates.rolling(entry_window, min_periods=1).sum() - 1
        entry_offset[:] = np.where(entry_dates==1, entry_offset, np.nan)
        entry_offset = entry_offset.unstack().dropna().reset_index()
        entry_offset.columns = ['SecCode', 'Date', 'T']
        return pd.merge(entry_offset, data)

    def _make_exit_dict(self, data, response_days):
        assert 'EARNINGSFLAG' in data.columns
        ernflag = data.pivot(index='Date', columns='SecCode',
                             values='EARNINGSFLAG')
        exit_dict = {}
        for i in range(1, self._entry_window + 1):
            shifted = ernflag.shift(response_days + i).fillna(0)     
            shifted.iloc[-1] = 1
            shifted = shifted.unstack().reset_index()
            shifted.columns = ['SecCode', 'Date', 'ExitFlag']
            shifted = shifted[shifted.ExitFlag == 1]
            exit_dict[i] = {k: g["SecCode"].tolist() for k, g
                                in shifted.groupby("Date")}

        return exit_dict

    def _make_group_dict(self, data):
        assert 'GGROUP' in data.columns
        data = data[['SecCode', 'GGROUP']].drop_duplicates()
        g_dict = {k: g["GGROUP"].tolist() for k,g in data.groupby("SecCode")}

        for code, group in g_dict.iteritems():
            if len(group) > 1:
                g_dict[code] = [x for x in group if not np.isnan(x)]

        return g_dict

    def _add_response_variables(self, data, response_days):
        return data.merge(fixed_response(data, days=response_days))

    def _trim_training_data(self, data, training_qtrs):
        if training_qtrs == -99:
            return data
        inds = create_time_index(data.Date)
        max_ind = np.max(inds)
        return data.iloc[inds > (max_ind - training_qtrs)]


def read_spy_data(spy_path=None):
    if spy_path is None:
        spy_path = SPY_PATH
    spy = pd.read_csv(spy_path)
    spy.Date = convert_date_array(spy.Date)
    return spy
