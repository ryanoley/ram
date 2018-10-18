import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.sandbox.base.features import *
from gearbox import create_time_index, convert_date_array
from ram.strategy.sandbox.base.utils import make_variable_dict


SPY_PATH = os.path.join(os.getenv('DATA'), 'ram', 'prepped_data',
                        'Sandbox', 'etfs', 'spy.csv')


class DataContainer1(object):

    def __init__(self):
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._processed_pricing_data = pd.DataFrame()
        self._set_features()
        self.read_mkt_data()

    def get_args(self):
        return {
            'train_pers': [20, -99]
        }

    def read_mkt_data(self):
        spy = _read_mkt_data()
        spy.loc[:, 'SecCode'] = 'HEDGE'
        spy['SplitMultiplier'] = spy.SplitFactor.pct_change().fillna(0) + 1
        self._market_data = spy

    def add_data(self, data):
        """
        Takes in raw data, processes it and caches it
        """
        data = self.create_features(data)
        data.dropna(subset=self.features, inplace=True)

        # Get data for daily pl calculations
        pricing_data = data[data.TestFlag].copy()
        pricing_cols = ['Date', 'SecCode', 'GGROUP', 'TM1', 'RClose', 'RVwap',
                        'RCashDividend', 'SplitMultiplier','AvgDolVol']

        # Separate training from test data
        train_data = self._processed_train_data.append(data[~data.TestFlag])
        train_data.dropna(subset=self.ret_cols, inplace=True)

        self._processed_train_data = train_data
        self._processed_test_data = data[data.TestFlag]
        self._processed_pl_data = pricing_data[pricing_cols]

    def create_features(self, data):
        # SPLITS
        data = create_split_multiplier(data)
        data.TM1 = convert_date_array(data.TM1)
        data['prtgt_discount'] = (data.RClose / data.PTARGETUNADJ) - 1

        # Open Return
        data['OpenRet'] = (data.LEAD1_AdjOpen / data.AdjClose) - 1

        # State of the World
        data['Abv_PRMA10'] = (data.PRMAH10_AdjClose > 0.).astype(int)
        data['Blw_PRMA10'] = (data.PRMAH10_AdjClose < 0.).astype(int)
        data['Abv_PRMA20'] = (data.PRMAH20_AdjClose > 0.).astype(int)
        data['Blw_PRMA20'] = (data.PRMAH20_AdjClose < 0.).astype(int)

        # 4 Day High/Low
        data = n_day_high_low(data, 'AdjClose', 4, 'CloseMax4')
        data = n_day_high_low(data, 'AdjClose', 4, 'CloseMin4', low=True)

        data = n_day_high_low(data, 'VOL20_AdjClose', 4, 'VolMax4')
        data = n_day_high_low(data, 'VOL20_AdjClose', 4, 'VolMin4', low=True)

        data = n_day_high_low(data, 'RSI20_AdjClose', 4, 'RSIMax4')
        data = n_day_high_low(data, 'RSI20_AdjClose', 4, 'RSIMin4', low=True)

        data = n_day_high_low(data, 'MFI20_AdjClose', 4, 'MFIMax4')
        data = n_day_high_low(data, 'MFI20_AdjClose', 4, 'MFIMin4', low=True)

        data = n_day_high_low(data, 'BOLL20_AdjClose', 4, 'BOLLMax4')
        data = n_day_high_low(data, 'BOLL20_AdjClose', 4, 'BOLLMin4', low=True)

        # Top/Bottom 33%
        data = n_pct_top_btm(data, 'PRMAH20_AdjClose', 33, 'Top33_PRMA20')
        data = n_pct_top_btm(data, 'PRMAH20_AdjClose', 33, 'Btm33_PRMA20',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'PRMAH60_AdjClose', 33, 'Top33_PRMA60')
        data = n_pct_top_btm(data, 'PRMAH60_AdjClose', 33, 'Btm33_PRMA60',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'VOL60_AdjClose', 33, 'Top33_VOL60')
        data = n_pct_top_btm(data, 'VOL60_AdjClose', 33, 'Btm33_VOL60',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'DISCOUNT252_AdjClose', 33, 'Top33_DISC252')
        data = n_pct_top_btm(data, 'DISCOUNT252_AdjClose', 33, 'Btm33_DISC252',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'MFI60_AdjClose', 33, 'Top33_MFI60')
        data = n_pct_top_btm(data, 'MFI60_AdjClose', 33, 'Btm33_MFI60',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'RSI60_AdjClose', 33, 'Top33_RSI60')
        data = n_pct_top_btm(data, 'RSI60_AdjClose', 33, 'Btm33_RSI60',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'BOLL60_AdjClose', 33, 'Top33_BOLL60')
        data = n_pct_top_btm(data, 'BOLL60_AdjClose', 33, 'Btm33_BOLL60',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'SIRANK', 33, 'Top33_SIRANK')
        data = n_pct_top_btm(data, 'SIRANK', 33, 'Btm33_SIRANK',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'ARM', 33, 'Top33_ARM')
        data = n_pct_top_btm(data, 'ARM', 33, 'Btm33_ARM',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'SISHORTSQUEEZE', 33, 'Top33_SHORTSQZ')
        data = n_pct_top_btm(data, 'SISHORTSQUEEZE', 33, 'Btm33_SHORTSQZ',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'prtgt_discount', 33, 'Top33_PXTGT')
        data = n_pct_top_btm(data, 'prtgt_discount', 33, 'Btm33_PXTGT',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'PE', 33, 'Top33_PE')
        data = n_pct_top_btm(data, 'PE', 33, 'Btm33_PE',
                             btm_pct=True)

        data = n_pct_top_btm(data, 'SALESGROWTHTTM', 33, 'Top33_REVGRWTH')
        data = n_pct_top_btm(data, 'SALESGROWTHTTM', 33, 'Btm33_REVGRWTH',
                             btm_pct=True)

        # Add multiple Returns for model training
        data = get_vwap_returns(data, 3, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 5, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 8, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 10, hedged=True,
                                market_data=self._market_data)
        data = get_vwap_returns(data, 15, hedged=True,
                                market_data=self._market_data)

        return data

    def prep_data(self, train_pers):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        # Fresh copies of processed raw data
        train_data, test_data, daily_pl = self._get_train_test_daily_data()

        self.train_data = self._trim_training_data(train_data, train_pers)
        self.test_data = test_data
        daily_pl.sort_values('Date', inplace=True)
        self.test_dates = daily_pl[['Date', 'TM1']].drop_duplicates()

        # Process implementation details
        self.close_dict = make_variable_dict(daily_pl, 'RClose')
        self.vwap_dict = make_variable_dict(daily_pl, 'RVwap')
        self.dividend_dict = make_variable_dict(daily_pl, 'RCashDividend', 0)
        self.split_mult_dict = make_variable_dict(daily_pl, 'SplitMultiplier',
                                                  1)

    def _set_features(self, inp_features=None):
        features = [
            # Technicals
            'PRMAH10_AdjClose', 'PRMAH20_AdjClose', 'PRMAH60_AdjClose',
            'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
            'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose',
            'MFI60_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose',
            'RSI60_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
            'BOLL60_AdjClose', 'OpenRet',
            # Starmine Short Interest
            'SIRANK', 'SIMARKETCAPRANK', 'SISECTORRANK',
            'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',
            # ARM
            'ARM', 'ARMEARNINGS', 'ARMEXRECS',
            # Discount to Price Target
            'prtgt_discount', 'RECMEAN',
            # Accounting Variables
            'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',
            'SALESGROWTHQ', 'SALESGROWTHTTM',
            'ADJEPSGROWTHQ', 'ADJEPSGROWTHTTM',
            'GROSSMARGINTTM', 'PE'
            ]

        self.ret_cols = ['Ret3', 'Ret5', 'Ret8', 'Ret10', 'Ret15']

        if inp_features is None:
            self.features = features
        else:
            self.features = inp_features
        return

    ###########################################################################

    def _get_train_test_daily_data(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy(),
            self._processed_pl_data.copy(),
        )

    def get_pricing_dicts(self, date, mkt_prices=False):
        vwaps = self.vwap_dict[date]
        closes = self.close_dict[date]
        dividends = self.dividend_dict[date]
        splits = self.split_mult_dict[date]
        return vwaps, closes, dividends, splits

    def _trim_training_data(self, data, train_pers):
        if (train_pers == -99) | (len(data) == 0):
            return data
        inds = create_time_index(data.Date)
        max_ind = np.max(inds)
        trim_data = data.iloc[inds > (max_ind - train_pers)].copy()
        return trim_data.reset_index(drop=True)

def _read_mkt_data(spy_path=None):
    if spy_path is None:
        spy_path = SPY_PATH
    spy = pd.read_csv(spy_path)
    spy.Date = convert_date_array(spy.Date)
    return spy
