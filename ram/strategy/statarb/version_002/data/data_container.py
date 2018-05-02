import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.data.feature_creator import *

from ram.strategy.statarb.abstract.data_container import BaseDataContainer
from ram.strategy.statarb.version_002.constructor.constructor import \
    PortfolioConstructor

LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3


class DataContainer(BaseDataContainer):

    def __init__(self):
        # Deliverable
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._features = []
        # Version specific
        self._response_days_args = [5, 10]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_args(self):
        return {
            'response_days': self._response_days_args,
            'response_type': ['Simple']
        }

    def set_args(self,
                 response_days,
                 response_type,
                 live_flag=False):
        self._response_days = response_days
        self._response_type = response_type
        train_data, test_data = self._get_train_test()

        if live_flag:
            self._test_data = test_data
            return

        # Add training flag given number of response days
        training_dates = train_data.Date.unique()
        training_dates = training_dates[:-response_days].copy()
        train_data = train_data[train_data.Date.isin(training_dates)]
        self._train_data = train_data
        self._test_data = test_data
        self._train_data_responses = train_data[
            'Response_{}_{}'.format(response_type, response_days)]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_train_data(self):
        return self._train_data

    def get_train_responses(self):
        return self._train_data_responses

    def get_train_features(self):
        return self._features

    def get_test_data(self):
        return self._test_data

    def get_test_dates(self):
        return self._test_dates

    def get_constructor_data(self):
        return self._constructor_data

    def get_pricing_data(self):
        return self._pricing_data

    def get_other_data(self):
        return self._other_data

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def prep_live_data(self, data, market_data):
        """
        This is data that is as of yesterday
        """
        data['TimeIndex'] = -1
        data['keep_inds'] = \
            (data.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (data.RClose >= LOW_PRICE_FILTER)
        prepped_data, prepped_features = self._make_features(data,
                                                             live_flag=True)
        market_data = market_data[['SecCode', 'Date', 'AdjClose']].copy()
        market_data = market_data[market_data.SecCode.isin(['50311', '11113'])]
        self._live_prepped_data = {}
        self._live_prepped_data['raw_data'] = data
        self._live_prepped_data['market_data'] = market_data
        self._live_prepped_data['prepped_data'] = prepped_data
        self._live_prepped_data['prepped_features'] = prepped_features
        self._constructor_data = {}

    def process_live_data(self, live_pricing_data):
        data = self._live_prepped_data['raw_data'].copy()
        market_data = self._live_prepped_data['market_data'].copy()
        prepped_data = self._live_prepped_data['prepped_data'].copy()
        prepped_features = list(self._live_prepped_data['prepped_features'])

        # Pop index pricing
        live_market = live_pricing_data[
            live_pricing_data.SecCode.isin(['50311', '11113'])]
        live_pricing_data = live_pricing_data[
            ~live_pricing_data.SecCode.isin(['50311', '11113'])]
        # Get live data for sec codes in this data set
        live_pricing_data = live_pricing_data[
            live_pricing_data.SecCode.isin(data.SecCode.unique())].copy()
        # Check no zero values
        no_data = live_pricing_data.Ticker[live_pricing_data.AdjClose == 0]
        if len(no_data):
            print('NO DATA FOR FOLLOWING TICKERS:')
            print(no_data.tolist())
            live_pricing_data = live_pricing_data.replace(0, np.nan)
        # Process data
        data = merge_live_pricing_data(data, live_pricing_data)
        market_data = merge_live_pricing_market_data(market_data, live_market)
        data = adjust_todays_prices(data)
        # Calculate avgdolvol
        data = calculate_avgdolvol(data)
        # Cleanup
        data = self._initial_clean(data, -1)
        # Technical variable calculation
        data_tech, features_tech = self._make_technical_features(
            data, live_flag=True)
        # Merge technical and non-technical
        pdata = prepped_data.merge(data_tech)
        features = prepped_features + features_tech
        # Separate training from test data
        self._processed_train_data = pd.DataFrame({'Date': [0]})
        self._processed_test_data = pdata
        self._features = features
        # TODO: is this needed
        self._test_dates = [dt.date.today()]
        # Process some data
        score_vars = PortfolioConstructor().get_args()['score_var']
        self._other_data = pdata[['SecCode', 'Date', 'keep_inds']+score_vars]
        # self._pricing_data = data[data.TestFlag][['SecCode', 'Date',
        #                                           'MarketCap', 'AvgDolVol',
        #                                           'RClose', 'RCashDividend',
        #                                           'SplitMultiplier']]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def process_training_data(self, data, market_data, time_index):
        # First cleanup
        data = self._initial_clean(data, time_index)

        # Create process training data, and get features
        adata, features_a = self._make_features(data)
        tdata, features_t = self._make_technical_features(data)
        responses = self._make_responses(data)

        pdata = data[['SecCode', 'Date', 'TestFlag', 'TimeIndex',
                      'keep_inds']].merge(adata).merge(tdata).merge(responses)
        features = features_a + features_t

        # Trim to just one quarter's data for training
        pdata = self._trim_to_one_month(pdata)

        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(pdata[~pdata.TestFlag])
        self._processed_test_data = pdata[pdata.TestFlag]
        self._features = features

        test_dates = data.Date[data.TestFlag].unique()
        test_dates = [t for t in test_dates if t.month == test_dates[0].month]
        self._test_dates = test_dates

        # Process some data
        score_vars = PortfolioConstructor().get_args()['score_var']
        self._pricing_data = data[data.TestFlag][['SecCode', 'Date',
                                                  'MarketCap', 'AvgDolVol',
                                                  'RClose', 'RCashDividend',
                                                  'SplitMultiplier']]
        self._other_data = \
            pdata[pdata.TestFlag][['SecCode', 'Date', 'keep_inds']+score_vars]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_responses(self, data):
        for i, days in enumerate(self._response_days_args):
            temp = simple_responses(data, days=days).merge(
                smoothed_responses(data, days=days))
            if i == 0:
                responses = temp
            else:
                responses = responses.merge(temp)
        return responses

    def _make_features(self, data, live_flag=False):
        """
        Makes fundamental features. Separated for speed during implementation.

        Parameters
        ----------
        data : pd.DataFrame
            Entire dataframe that is available from a file, training and
            test data.
        """
        feat = FeatureAggregator()

        for feature in accounting_features:
            if live_flag:
                temp = clean_pivot_raw_data(data, feature, lag=0)
                temp = temp.iloc[-1:]
                temp.index = [0]
            else:
                temp = clean_pivot_raw_data(data, feature, lag=1)
            feat.add_feature(data_rank(temp), feature, backfill=True)

        for feature in starmine_features:
            if live_flag:
                temp = clean_pivot_raw_data(data, feature, lag=0)
                temp = temp.iloc[-1:]
                temp.index = [0]
            else:
                temp = clean_pivot_raw_data(data, feature, lag=1)
            feat.add_feature(data_rank(temp), feature, backfill=True)

        pdata = data[['SecCode', 'Date']].copy()
        n_id_features = pdata.shape[1]  # For capturing added features
        if live_flag:
            max_date = pdata.Date.max()
            pdata = pdata[pdata.Date == max_date]
            pdata.Date = 0

        pdata = pdata.merge(feat.make_dataframe())

        # Manually handle SI Data
        si_vars = ['SIINSTOWNERSHIP', 'SIMARKETCAPRANK', 'SIRANK',
                   'SISECTORRANK', 'SISHORTSQUEEZE', 'SIUNADJRANK']

        pdata.loc[:, si_vars] = pdata.loc[:, si_vars].fillna(0.5)
        features = pdata.columns[n_id_features:].tolist()
        if live_flag:
            pdata.Date = dt.date.today()
        return pdata, features

    def _make_technical_features(self, data, live_flag=False):
        # TECHNICAL VARIABLES
        # Clean and format data points
        open_ = clean_pivot_raw_data(data, 'AdjOpen')
        high = clean_pivot_raw_data(data, 'AdjHigh')
        low = clean_pivot_raw_data(data, 'AdjLow')
        close = clean_pivot_raw_data(data, 'AdjClose')
        volume = clean_pivot_raw_data(data, 'AdjVolume')
        avgdolvol = clean_pivot_raw_data(data, 'AvgDolVol')

        feat = FeatureAggregator()

        # Daily returns
        day_ret = data_rank(close / open_)
        if live_flag:
            day_ret = day_ret.iloc[-1]
            day_ret.name = 0
        feat.add_feature(day_ret, 'day_ret')

        # PRMA vals
        prma = PRMA(live_flag)
        for i in [5, 10, 15, 20]:
            feat.add_feature(data_rank(prma.fit(close, i)),
                             'prma_{}'.format(i))

        vol = VOL(live_flag)
        for i in [10, 20, 40]:
            feat.add_feature(data_rank(vol.fit(close, i)),
                             'vol_{}'.format(i))

        disc = DISCOUNT(live_flag)
        for i in [30, 40]:
            feat.add_feature(data_rank(disc.fit(close, i)),
                             'disc_{}'.format(i))

        bol = BOLL(live_flag)
        for i in [10, 20, 40]:
            feat.add_feature(data_rank(bol.fit(close, i)),
                             'boll_{}'.format(i))

        rsi = RSI(live_flag)
        for i in [15, 30]:
            feat.add_feature(data_rank(rsi.fit(close, i)),
                             'rsi_{}'.format(i))

        mfi = MFI(live_flag)
        for i in [15, 30]:
            feat.add_feature(data_rank(mfi.fit(high, low, close, volume, i)),
                             'mfi_{}'.format(i))

        # Smoothed prma
        ret = data_rank(prma.fit(close, 10) / prma.fit(close, 2))
        if live_flag:
            ret = ret.iloc[-1]
            ret.name = 0
        feat.add_feature(ret, 'prma_2_10')

        ret = data_rank(prma.fit(close, 10) / prma.fit(close, 3))
        if live_flag:
            ret = ret.iloc[-1]
            ret.name = 0
        feat.add_feature(ret, 'prma_3_10')

        ret = data_rank(prma.fit(close, 20) / prma.fit(close, 4))
        if live_flag:
            ret = ret.iloc[-1]
            ret.name = 0
        feat.add_feature(ret, 'prma_4_20')

        ret = data_rank(prma.fit(close, 30) / prma.fit(close, 5))
        if live_flag:
            ret = ret.iloc[-1]
            ret.name = 0
        feat.add_feature(ret, 'prma_5_30')

        # LONG returns
        for i in [10, 20, 40]:
            ret = data_rank(close.pct_change(i))
            if live_flag:
                ret = ret.iloc[-1]
                ret.name = 0
            feat.add_feature(ret, 'ret_{}d'.format(i))

        # Create output
        if live_flag:
            pdata = data[['SecCode', 'Date', 'keep_inds']].copy()
        else:
            pdata = data[['SecCode', 'Date']].copy()

        n_id_features = pdata.shape[1]  # For capturing added features
        # Adjust date for faster live imp
        if live_flag:
            max_date = pdata.Date.max()
            pdata = pdata[pdata.Date == max_date].copy()
            pdata.Date = 0
        pdata = pdata.merge(feat.make_dataframe())

        # Extract features
        features = pdata.columns[n_id_features:].tolist()

        if live_flag:
            pdata.Date = dt.date.today()
        return pdata, features

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initial_clean(self, data, time_index):
        data = data.sort_values(['SecCode', 'Date']).reset_index(drop=True)
        data['TimeIndex'] = time_index
        data = create_split_multiplier(data)
        data['keep_inds'] = \
            (data.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (data.RClose >= LOW_PRICE_FILTER)
        return data

    def _trim_to_one_month(self, data):
        # Trim only one quarter's worth of training data
        max_date = data.Date[~data.TestFlag].max()
        trim_date = dt.date(max_date.year, max_date.month, 1)
        data = data[data.Date >= trim_date].copy()
        return data

    def _get_train_test(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy()
        )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def create_split_multiplier(data):
    # SPLITS: Instead of using the levels, use the CHANGE in levels.
    # This is necessary for the updating of positions and prices downstream
    data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) * \
        (data.SecCode == data.SecCode.shift(1)).astype(int) + 1
    del data['SplitFactor']
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def merge_live_pricing_data(data, live_pricing_data):
    # Add missing columns to live pricing data, and merge
    new_cols = list(set(data.columns) - set(live_pricing_data.columns))
    live_pricing_data = live_pricing_data.join(
        pd.DataFrame(columns=new_cols), how='left')
    live_pricing_data['Date'] = dt.date.today()
    live_pricing_data['TestFlag'] = True
    return data.append(live_pricing_data)[data.columns].reset_index(drop=True)


def merge_live_pricing_market_data(data, live_pricing_data):
    # Add missing columns to live pricing data, and merge
    live_pricing_data = live_pricing_data[['SecCode', 'AdjClose']].copy()
    live_pricing_data['Date'] = dt.date.today()
    return data.append(live_pricing_data)[data.columns].reset_index(drop=True)


def adjust_todays_prices(data):
    data.AdjVwap = np.where(data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
    data.RClose = np.where(data.RClose.isnull(), data.AdjClose, data.RClose)
    return data


def calculate_avgdolvol(data, days=30):
    vwap = data.pivot(index='Date', columns='SecCode', values='AdjVwap')
    vol = data.pivot(index='Date', columns='SecCode', values='AdjVolume')
    dolvol = vol * vwap
    avgdolvol = dolvol.rolling(days).mean().unstack().reset_index()
    avgdolvol.columns = ['SecCode', 'Date', 'AvgDolVol']
    if 'AvgDolVol' in data.columns:
        data = data.drop('AvgDolVol', axis=1)
    return data.merge(avgdolvol)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def simple_responses(data, days=2):
    """
    Just return 1 or 0 for Position or Negative return
    """
    assert isinstance(days, int)
    close = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    rets = (close.pct_change(days)
            .shift(-days).rank(axis=1, pct=True) >= 0.5).astype(int)
    output = rets.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response_Simple_{}'.format(days)]
    return output


def smoothed_responses(data, days=2):
    assert isinstance(days, int)
    close = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    for i in range(1, days+1):
        if i == 1:
            rank = close.pct_change(i).shift(-i).rank(axis=1, pct=True)
        else:
            rank += close.pct_change(i).shift(-i).rank(axis=1, pct=True)
    output = (rank.rank(axis=1, pct=True) >= 0.5).astype(int)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response_Smoothed_{}'.format(days)]
    return output


def rank_filter_data(data, value_column, keep_index, rank=True):
    data = data[['SecCode', 'Date', value_column]].copy()
    data.loc[~keep_index, value_column] = np.nan
    data = data.pivot(index='Date', columns='SecCode',
                      values=value_column)
    if rank:
        return data.rank(axis=1, pct=True)
    else:
        return data


accounting_features = [
    'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',

    'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',

    'EBITGROWTHQ', 'EBITGROWTHTTM',

    'SALESGROWTHQ', 'SALESGROWTHTTM',

    'FREECASHFLOWGROWTHQ', 'FREECASHFLOWGROWTHTTM',

    'PE'
]


starmine_features = [
    'ARM', 'ARMREVENUE', 'ARMRECS',
    'ARMEARNINGS', 'ARMEXRECS', 'SIRANK',
    'SIMARKETCAPRANK', 'SISECTORRANK',
    'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',
]
