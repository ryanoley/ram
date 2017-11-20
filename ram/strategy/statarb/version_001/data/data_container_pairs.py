import os
import numpy as np
import pandas as pd
import datetime as dt

from scipy.stats.mstats import winsorize

from ram.data.feature_creator import *

from ram.strategy.statarb.abstract.data_container import BaseDataContainer

from ram.strategy.statarb.version_001.data.pairs_selector import PairSelector
from ram.strategy.statarb.version_001.data.pairs_selector_filter import \
    PairSelectorFilter


class DataContainerPairs(BaseDataContainer):

    def __init__(self):
        self._time_index_data_for_responses = {}
        self._time_index_response_data = {}
        # Deliverable
        self._processed_train_data = pd.DataFrame()
        self._processed_train_responses = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._features = []

    def get_args(self):
        return {
            'response_params': [
                {'type': 'smoothed',
                 'response_days': [2, 4, 6],
                 'response_thresh': 0.3},
                {'type': 'smoothed',
                 'response_days': [2],
                 'response_thresh': 0.4},
                {'type': 'simple', 'response_days': 2},
            ],
            'training_qtrs': [-99, 20]
        }

    def set_args(self, time_index, response_params, training_qtrs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        response_cols = ['SecCode', 'Date', 'TimeIndex',
                         self._response_arg_map[str(response_params)]]
        responses = self._processed_train_responses[response_cols].copy()
        responses.columns = ['SecCode', 'Date', 'TimeIndex', 'Response']
        # Fresh copies of processed raw data
        train_data, test_data = self._get_train_test()
        # Adjust per hyperparameters
        train_data, responses = self._trim_training_response_data(
            train_data, responses, training_qtrs)
        # Make sure training and responses are aligned
        responses = train_data[['SecCode', 'TimeIndex', 'Date']].merge(
            responses, how='left')
        assert np.all(train_data.SecCode.values == responses.SecCode.values)
        assert np.all(train_data.Date.values == responses.Date.values)
        responses.index = train_data.index
        # Merge response data
        self.train_data = train_data
        self.train_data_responses = responses
        self.test_data = test_data

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_training_data(self):
        return self.train_data

    def get_training_responses(self):
        return self.train_data_responses

    def get_training_feature_names(self):
        return self._features

    def get_test_data(self):
        return self.test_data

    def get_simulation_feature_dictionary(self):
        return self.constructor_data

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def prep_live_data(self):
        pass

    def process_live_data(self):
        pass

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def process_training_data(self, data, time_index):
        # First cleanup
        data = self._initial_clean(data, time_index)
        # Get pairs from 252 days worth of data
        pair_info, zscores = self._get_pairs_info(data)
        # Create process training data, and get features
        pdata, features = self._make_features(data)
        # Trim to just one quarter's data
        pdata = self._trim_to_one_quarter(pdata)
        # Merge market data
        pdata, features = self._merge_market_data(pdata, features)
        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(pdata[~pdata.TestFlag])
        self._processed_test_data = pdata[pdata.TestFlag]
        self._features = features
        # Process responses - Get order correct
        data2 = pdata[['SecCode', 'Date', 'TimeIndex', 'TestFlag']].merge(
            data[['SecCode', 'Date', 'AdjClose']], how='left')
        assert np.all(data2.SecCode == pdata.SecCode)
        assert np.all(data2.Date == pdata.Date)
        self._make_responses(data2)
        # Process some data
        self.constructor_data = {
            'pricing': data[data.TestFlag][['MarketCap', 'AvgDolVol',
                                            'RClose', 'RCashDividend',
                                            'SplitMultiplier']],
            'pair_info': pair_info,
            'zscores': zscores
        }

    def process_training_market_data(self, data):
        if hasattr(self, '_market_data'):
            return
        # index name map at the bottom of the file
        data = data.merge(index_name_map)
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
            pdata.columns = ['MKT_{}_{}'.format(
                col, f.replace('_AdjClose', '')) for col in pdata.columns]
            market_data = market_data.join(pdata, how='outer')
        # Nan Values set to medians of rows
        market_data = market_data.fillna(market_data.median())
        self._market_data = market_data.reset_index()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_responses(self, data):
        # Just training data
        data = data[~data.TestFlag]
        # Response arg map
        self._response_arg_map = {}
        responses = data[['SecCode', 'TimeIndex', 'Date']]
        for i, args in enumerate(self.get_args()['response_params']):
            self._response_arg_map[str(args)] = i
            # Get name
            if args['type'] == 'smoothed':
                resp = smoothed_responses(
                    data,
                    thresh=args['response_thresh'],
                    days=args['response_days'])

            elif args['type'] == 'simple':
                resp = simple_responses(data, days=args['response_days'])
            resp.columns = ['SecCode', 'Date', i]
            responses = responses.merge(resp)
        # Append
        self._processed_train_responses = \
            self._processed_train_responses.append(responses)

    def _make_features(self, data, live_flag=False):
        # TECHNICAL VARIABLES
        # Clean and format data points
        open_ = clean_pivot_raw_data(data, 'AdjOpen')
        high = clean_pivot_raw_data(data, 'AdjHigh')
        low = clean_pivot_raw_data(data, 'AdjLow')
        close = clean_pivot_raw_data(data, 'AdjClose')
        volume = clean_pivot_raw_data(data, 'AdjVolume')
        avgdolvol = clean_pivot_raw_data(data, 'AvgDolVol')

        # Set correct method for training or live implementation
        prma = PRMA(live_flag)
        boll = BOLL(live_flag)
        mfi = MFI(live_flag)
        rsi = RSI(live_flag)
        vol = VOL(live_flag)
        discount = DISCOUNT(live_flag)

        pdata = data[['SecCode', 'Date', 'TimeIndex', 'TestFlag']]

        v = outlier_rank(prma.fit(close, 10))[0]
        pdata = pdata.merge(unstack_label_data(v, 'PRMA10'))
        v = outlier_rank(prma.fit(close, 20))[0]
        pdata = pdata.merge(unstack_label_data(v, 'PRMA20'))
        v = outlier_rank(prma.fit(close, 60))[0]
        pdata = pdata.merge(unstack_label_data(v, 'PRMA60'))

        v = outlier_rank(prma.fit(avgdolvol, 10))[0]
        pdata = pdata.merge(unstack_label_data(v, 'PRMA10_AvgDolVol'))
        v = outlier_rank(prma.fit(avgdolvol, 20))[0]
        pdata = pdata.merge(unstack_label_data(v, 'PRMA20_AvgDolVol'))
        v = outlier_rank(prma.fit(avgdolvol, 60))[0]
        pdata = pdata.merge(unstack_label_data(v, 'PRMA60_AvgDolVol'))

        v = outlier_rank(boll.fit(close, 10))[0]
        pdata = pdata.merge(unstack_label_data(v, 'BOLL10'))
        v = outlier_rank(boll.fit(close, 20))[0]
        pdata = pdata.merge(unstack_label_data(v, 'BOLL20'))
        v = outlier_rank(boll.fit(close, 60))[0]
        pdata = pdata.merge(unstack_label_data(v, 'BOLL60'))

        v = outlier_rank(mfi.fit(high, low, close, volume, 10))[0]
        pdata = pdata.merge(unstack_label_data(v, 'MFI10'))
        v = outlier_rank(mfi.fit(high, low, close, volume, 20))[0]
        pdata = pdata.merge(unstack_label_data(v, 'MFI20'))
        v = outlier_rank(mfi.fit(high, low, close, volume, 60))[0]
        pdata = pdata.merge(unstack_label_data(v, 'MFI60'))

        v = outlier_rank(rsi.fit(close, 10))[0]
        pdata = pdata.merge(unstack_label_data(v, 'RSI10'))
        v = outlier_rank(rsi.fit(close, 20))[0]
        pdata = pdata.merge(unstack_label_data(v, 'RSI20'))
        v = outlier_rank(rsi.fit(close, 60))[0]
        pdata = pdata.merge(unstack_label_data(v, 'RSI60'))

        v = outlier_rank(vol.fit(close, 10))[0]
        pdata = pdata.merge(unstack_label_data(v, 'VOL10'))
        v = outlier_rank(vol.fit(close, 20))[0]
        pdata = pdata.merge(unstack_label_data(v, 'VOL20'))
        v = outlier_rank(vol.fit(close, 60))[0]
        pdata = pdata.merge(unstack_label_data(v, 'VOL60'))

        v = outlier_rank(discount.fit(close, 63))[0]
        pdata = pdata.merge(unstack_label_data(v, 'DISCOUNT63'))
        v = outlier_rank(discount.fit(close, 126))[0]
        pdata = pdata.merge(unstack_label_data(v, 'DISCOUNT126'))

        # ACCOUNTING, STARMINE, IBES
        if live_flag:
            # If live, need to cut down on size of data for speed?
            lag = 0
        else:
            lag = 1

        # Ranked features
        for feature in accounting_features:
            v = outlier_rank(clean_pivot_raw_data(data, feature, lag))[0]
            pdata = pdata.merge(unstack_label_data(v, feature))
        for feature in starmine_features:
            v = outlier_rank(clean_pivot_raw_data(data, feature, lag))[0]
            pdata = pdata.merge(unstack_label_data(v, feature))

        # IBES Ranked
        v = outlier_rank(clean_pivot_raw_data(data, 'IBES_Discount',
                                              lag))[0]
        pdata = pdata.merge(unstack_label_data(v, 'IBES_Discount'))

        v = outlier_rank(clean_pivot_raw_data(data, 'IBES_Discount_Smooth',
                                              lag))[0]
        pdata = pdata.merge(unstack_label_data(v, 'IBES_Discount_Smooth'))

        # IBES Not Ranked
        v = clean_pivot_raw_data(data, 'IBES_Target_Increase', lag)
        pdata = pdata.merge(unstack_label_data(v, 'IBES_Target_Increase'))

        v = clean_pivot_raw_data(data, 'IBES_Target_Decrease', lag)
        pdata = pdata.merge(unstack_label_data(v, 'IBES_Target_Decrease'))

        # Earnings related variables
        pdata = pdata.merge(data[['SecCode', 'Date', 'EARNINGS_Blackout']])
        pdata = pdata.merge(data[['SecCode', 'Date', 'EARNINGS_AnchorRet']])
        pdata = pdata.merge(data[['SecCode', 'Date', 'EARNINGS_Ret']])

        # Extract features
        features = pdata.columns[3:].tolist()
        return pdata, features

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initial_clean(self, data, time_index):
        data['TimeIndex'] = time_index
        # SPLITS: Instead of using the levels, use the CHANGE in levels.
        # This is necessary for the updating of positions and prices downstream
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        data = data.drop('SplitFactor', axis=1)
        data['GGROUP'] = data.GGROUP.fillna(0).astype(int).astype(str)
        data['GSECTOR'] = data.GGROUP.apply(lambda x: x[:2])
        # IBES feature processing
        data = data.merge(make_ibes_increases_decreases(data))
        data = data.merge(make_ibes_discount(data))
        # Earnings related fields
        data = append_ern_date_blackout(data, offset1=-2, offset2=4)
        data = data.merge(make_ern_return(data))
        data = data.merge(make_anchor_ret(data))
        return data

    def _trim_to_one_quarter(self, data):
        # Trim only one quarter's worth of training data
        min_date = data.Date[data.TestFlag].min()
        trim_date = min_date - dt.timedelta(days=80)
        trim_date = dt.date(trim_date.year, trim_date.month, 1)
        data = data[data.Date >= trim_date].copy()
        return data

    def _get_pairs_info(self, data):
        pair_info, _, zscores = PairSelector().rank_pairs(
            data, 20, filter_n_pairs_per_seccode=30)
        zscores = zscores.loc[data.Date[data.TestFlag].unique()]
        return pair_info, zscores

    def _merge_market_data(self, pdata, features):
        pdata = pdata.merge(self._market_data, how='left').fillna(0)
        features_mkt = self._market_data.columns.tolist()
        features_mkt.remove('Date')
        features += features_mkt
        return pdata, features

    def _get_train_test(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy()
        )

    def _trim_training_response_data(self, data, responses, training_qtrs):
        if training_qtrs == -99:
            return data, responses
        max_ind = data.TimeIndex.max()
        inds1 = data.TimeIndex > (max_ind-training_qtrs)
        inds2 = responses.TimeIndex > (max_ind-training_qtrs)
        return data[inds1], responses[inds2]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_ibes_increases_decreases(data):
    """
    Creates two columns, which indicates freshness of Increase or Decrease
    in price targets. Day of change is worth 1 and increments down by
    .2 each day going forward.
    """
    price_targets = data.pivot(index='Date',
                               columns='SecCode',
                               values='PTARGETMEAN')
    target_changes = price_targets.pct_change().fillna(0)
    output1 = target_changes.copy()
    output1[:] = 0
    output2 = output1.copy()
    increases = (target_changes > 0).astype(int)
    decreases = (target_changes < 0).astype(int)
    for i in range(5):
        output1 += (increases.shift(i) * (1 - i*.2)).fillna(0)
        output2 += (decreases.shift(i) * (1 - i*.2)).fillna(0)
    output1 = output1.unstack().reset_index()
    output1.columns = ['SecCode', 'Date', 'IBES_Target_Increase']
    output2 = output2.unstack().reset_index()
    output2.columns = ['SecCode', 'Date', 'IBES_Target_Decrease']
    return output1.merge(output2)


def make_ibes_discount(data):
    data['IBES_Discount'] = winsorize(
        data.PTARGETUNADJ / data.RClose - 1, limits=(0.005, 0.005))
    discounts = data.pivot(index='Date',
                           columns='SecCode',
                           values='IBES_Discount')
    discounts_smooth = discounts.rolling(4).mean()
    discounts = discounts.unstack().reset_index()
    discounts.columns = ['SecCode', 'Date', 'IBES_Discount']
    discounts_smooth = discounts_smooth.unstack().reset_index()
    discounts_smooth.columns = ['SecCode', 'Date', 'IBES_Discount_Smooth']
    return discounts.merge(discounts_smooth)


def make_ern_return(data):
    """
    (T-1) to (T+1) Vwap return. Doesn't register until (T+2)
    """
    prices = data.pivot(index='Date', columns='SecCode',
                        values='AdjVwap').fillna(method='pad')
    earningsflag = data.pivot(index='Date', columns='SecCode',
                              values='EARNINGSFLAG').fillna(0)
    rets = prices.shift(-1) / prices.shift(1) - 1
    rets[:] = np.where(earningsflag == 1, rets, np.nan)
    # Shift to (T+2)
    rets = rets.fillna(method='pad').shift(2).fillna(0)
    output = rets.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'EARNINGS_Ret']
    return output


def append_ern_date_blackout(data, offset1=-1, offset2=2):
    assert offset1 <= 0, 'Offset1 must be less than/equal to 0'
    assert offset2 >= 0, 'Offset2 must be greater than/equal to 0'
    ern_inds = np.where(data.EARNINGSFLAG == 1)[0]
    all_inds = ern_inds.copy()
    for i in range(abs(offset1)):
        all_inds = np.append(all_inds, ern_inds-(i+1))
    for i in range(offset2):
        all_inds = np.append(all_inds, ern_inds+(i+1))
    all_inds = all_inds[all_inds >= 0]
    all_inds = all_inds[all_inds < data.shape[0]]
    blackouts = np.zeros(data.shape[0])
    blackouts[all_inds] = 1
    data['EARNINGS_Blackout'] = blackouts
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_anchor_ret(data, init_offset=1, window=20):
    """
    Parameters
    ----------
    init_offset : int
        The index relative to the earnings date that represents the first
        anchor price
    window : int
        The maximum number of days to look back to create the anchor price
    """
    assert 'EARNINGS_Blackout' in data.columns
    closes = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    earningsflag = data.pivot(index='Date', columns='SecCode',
                              values='EARNINGSFLAG').fillna(0)
    blackout = data.pivot(index='Date', columns='SecCode',
                          values='EARNINGS_Blackout').fillna(0)
    # Get window period anchor price
    init_anchor = earningsflag.shift(init_offset).fillna(0) * closes
    end_anchor = earningsflag.shift(init_offset+window).fillna(0) * -1 * \
        closes.shift(window).fillna(0)
    init_anchor2 = (init_anchor + end_anchor).cumsum()
    output = closes.copy()
    output[:] = np.where(init_anchor2 == 0,
                         closes.shift(window-1), init_anchor2)
    output[:] = np.where(blackout, np.nan, output)
    output = (closes / output).unstack().reset_index().fillna(0)
    output.columns = ['SecCode', 'Date', 'EARNINGS_AnchorRet']
    return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def smoothed_responses(data, thresh=.25, days=[2, 4, 6]):
    if not isinstance(days, list):
        days = [days]
    rets = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    for i in days:
        if i == days[0]:
            rank = rets.pct_change(i).shift(-i).rank(axis=1, pct=True)
        else:
            rank += rets.pct_change(i).shift(-i).rank(axis=1, pct=True)
    final_ranks = rank.rank(axis=1, pct=True)
    output = final_ranks.copy()
    output[:] = (final_ranks >= (1 - thresh)).astype(int) - \
        (final_ranks <= thresh).astype(int)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response']
    return output


def simple_responses(data, days=2):
    """
    Just return 1 or 0 for Position or Negative return
    """
    assert isinstance(days, int)
    rets = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    rets2 = (rets.pct_change(days).shift(-days) >= 0).astype(int)
    output = rets2.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response']
    return output


accounting_features = [
    'NETINCOMEQ', 'NETINCOMETTM',
    'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',

    'OPERATINGINCOMEQ', 'OPERATINGINCOMETTM',
    'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',

    'EBITQ', 'EBITTTM',
    'EBITGROWTHQ', 'EBITGROWTHTTM',

    'SALESQ', 'SALESTTM',
    'SALESGROWTHQ', 'SALESGROWTHTTM',

    'FREECASHFLOWQ', 'FREECASHFLOWTTM',
    'FREECASHFLOWGROWTHQ', 'FREECASHFLOWGROWTHTTM',

    'GROSSPROFASSET', 'ASSETS',

    'GROSSMARGINTTM', 'EBITDAMARGIN',

    'PE', 'FCFMARKETCAP', 'CASHEV',
]


starmine_features = [
    'ARM', 'ARMREVENUE', 'ARMRECS',
    'ARMEARNINGS', 'ARMEXRECS', 'SIRANK',
    'SIMARKETCAPRANK', 'SISECTORRANK',
    'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',
]


ibes_features = [
    'IBES_Target_Increase', 'IBES_Target_Decrease',
    'IBES_Discount', 'IBES_Discount_Smooth'
]


index_name_map = pd.DataFrame([
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
