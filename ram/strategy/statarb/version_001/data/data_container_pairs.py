import os
import numpy as np
import pandas as pd
import datetime as dt

from scipy.stats.mstats import winsorize

from ram.data.feature_creator import *

from ram.strategy.statarb.abstract.data_container import BaseDataContainer

from ram.strategy.statarb.version_001.data.pairs_selector import PairSelector


class DataContainerPairs(BaseDataContainer):

    def __init__(self):
        self._time_index_data_for_responses = {}
        self._time_index_response_data = {}
        # Deliverable
        self._processed_train_data = pd.DataFrame()
        self._processed_train_responses = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._features = []

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_args(self):
        return {
            'response_params': [
                {'type': 'smoothed',
                 'response_days': [2, 4, 6],
                 'response_thresh': 0.3},
                {'type': 'smoothed',
                 'response_days': [2],
                 'response_thresh': 0.4},
            ],
            'training_qtrs': [-99, 20]
        }

    def set_args(self, response_params, training_qtrs):
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

    def get_train_data(self):
        return self.train_data

    def get_train_responses(self):
        return self.train_data_responses

    def get_train_features(self):
        return self._features

    def get_test_data(self):
        return self.test_data

    def get_constructor_data(self):
        return self._constructor_data

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def prep_live_data(self, data, market_data):
        self.process_training_market_data(market_data)
        pair_info = self._get_pairs_info(data)
        # Non pricing features
        data = self._make_earnings_data(data)
        data['TimeIndex'] = -1
        data_features_1, features_1 = self._make_features(data, live_flag=True)
        self._live_prepped_data = {}
        self._live_prepped_data['data'] = data
        self._live_prepped_data['features'] = features_1
        self._live_prepped_data['data_features_1'] = data_features_1

        self._constructor_data = {}
        self._constructor_data['pair_info'] = pair_info

    def process_live_data(self, live_pricing_data):
        """
        Notes:
        HOW DO WE HANDLE LIVE SPLITS??
        """
        data = self._live_prepped_data['data']
        features_1 = self._live_prepped_data['features']
        data_features_1 = self._live_prepped_data['data_features_1']

        del self._live_prepped_data

        # Get live data for sec codes in this data set
        live_pricing_data = live_pricing_data[
            live_pricing_data.SecCode.isin(data.SecCode.unique())].copy()

        # Check no zero values
        no_data = live_pricing_data.Ticker[live_pricing_data.AdjClose == 0]
        if len(no_data):
            print('NO DATA FOR FOLLOWING TICKERS:')
            print(no_data.tolist())
            live_pricing_data = live_pricing_data.replace(0, 10)
            #live_pricing_data = live_pricing_data.replace(0, np.nan)

        # Process data
        data = merge_live_pricing_data(data, live_pricing_data)
        data = adjust_todays_prices(data)

        # Cleanup
        data = self._initial_clean(data, -1)

        # Technical variable calculation
        data_features_2, features_2 = self._make_technical_features(
            data, live_flag=True)

        # Merge technical and non-technical
        pdata = data_features_1.merge(data_features_2)
        features = features_1 + features_2

        # Zscores - PERHAPS SHORTEN DATA FOR SPEED?
        zscores = self._get_pairs_z_score(data,
                                          self._constructor_data['pair_info'],
                                          live_flag=True)
        zscores = zscores.to_frame().T

        # Merge market data
        pdata, features = self._merge_market_data(pdata, features)
        self.test_data = pdata
        self._features = features
        self._constructor_data['zscores'] = zscores

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def process_training_data(self, data, time_index):
        # First cleanup
        data = self._initial_clean(data, time_index)
        data = self._make_earnings_data(data)

        # Pairs - finds best pairs over 252 days of data
        pair_info = self._get_pairs_info(data)
        zscores = self._get_pairs_z_score(data, pair_info)
        zscores = zscores.loc[data.Date[data.TestFlag].unique()]

        # Create process training data, and get features
        adata, features_a = self._make_features(data)
        tdata, features_t = self._make_technical_features(data)
        pdata = adata.merge(tdata)
        features = features_a + features_t

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
        assert np.all(self._processed_train_data.SecCode.values ==
                      self._processed_train_responses.SecCode.values)
        assert np.all(self._processed_train_data.Date.values ==
                      self._processed_train_responses.Date.values)
        # Process some data
        self._constructor_data = {
            'pricing': data[data.TestFlag][['SecCode', 'Date',
                                            'MarketCap', 'AvgDolVol',
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
        """
        Everything but Technical features. Separated for speed during
        implementation.
        """
        feat = FeatureAggregator()

        for feature in accounting_features:
            if live_flag:
                temp = clean_pivot_raw_data(data, feature, lag=0)
                temp = temp.iloc[-1:]
                temp.index = [0]
            else:
                temp = clean_pivot_raw_data(data, feature, lag=1)
            feat.add_feature(data_rank(temp), feature)

        for feature in starmine_features:
            if live_flag:
                temp = clean_pivot_raw_data(data, feature, lag=0)
                temp = temp.iloc[-1:]
                temp.index = [0]
            else:
                temp = clean_pivot_raw_data(data, feature, lag=1)
            feat.add_feature(data_rank(temp), feature)

        pdata = data[['SecCode', 'Date', 'TimeIndex', 'TestFlag']].copy()
        n_id_features = pdata.shape[1]  # For capturing added features
        if live_flag:
            max_date = pdata.Date.max()
            pdata = pdata[pdata.Date == max_date].copy()
            mdata = data[data.Date == max_date].copy()
            pdata.Date = 0
            mdata.Date = 0

        pdata = pdata.merge(feat.make_dataframe())

        if live_flag:
            # Earnings related variables
            pdata = pdata.merge(mdata[['SecCode', 'Date',
                                       'EARNINGS_Blackout']])
            pdata = pdata.merge(mdata[['SecCode', 'Date',
                                       'EARNINGS_AnchorRet']])
            pdata = pdata.merge(mdata[['SecCode', 'Date',
                                       'EARNINGS_Ret']])

        else:
            # Earnings related variables
            pdata = pdata.merge(data[['SecCode', 'Date',
                                      'EARNINGS_Blackout']])
            pdata = pdata.merge(data[['SecCode', 'Date',
                                      'EARNINGS_AnchorRet']])
            pdata = pdata.merge(data[['SecCode', 'Date',
                                      'EARNINGS_Ret']])

        features = pdata.columns[n_id_features:].tolist()
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

        # If live_flag adjust avg dol volume because it will have been a
        # nan that was fixed by clean_pivot_raw_data
        if live_flag:
            vwap = clean_pivot_raw_data(data, 'AdjVwap')
            new_dolvol = (volume.iloc[-1] * vwap.iloc[-1]) / 1e6
            avgdolvol.iloc[-1] = (avgdolvol.iloc[-2] * 30 + new_dolvol) / 31.
            # TEMP: print out average discount from day before
            print('AVG diff in avgdolvol: {}'.format(
                avgdolvol.iloc[-2:].pct_change().iloc[-1].mean()))

        # Set correct method for training or live implementation
        prma = PRMA(live_flag)
        boll = BOLL(live_flag)
        mfi = MFI(live_flag)
        rsi = RSI(live_flag)
        vol = VOL(live_flag)
        discount = DISCOUNT(live_flag)

        feat = FeatureAggregator()

        feat.add_feature(data_rank(prma.fit(close, 10)), 'PRMA10')
        feat.add_feature(data_rank(prma.fit(close, 20)), 'PRMA20')
        feat.add_feature(data_rank(prma.fit(close, 60)), 'PRMA60')

        feat.add_feature(data_rank(prma.fit(avgdolvol, 10)),
                         'PRMA10_AvgDolVol')
        feat.add_feature(data_rank(prma.fit(avgdolvol, 20)),
                         'PRMA20_AvgDolVol')
        feat.add_feature(data_rank(prma.fit(avgdolvol, 60)),
                         'PRMA60_AvgDolVol')

        feat.add_feature(data_rank(boll.fit(close, 10)), 'BOLL10')
        feat.add_feature(data_rank(boll.fit(close, 20)), 'BOLL20')
        feat.add_feature(data_rank(boll.fit(close, 60)), 'BOLL60')

        feat.add_feature(data_rank(mfi.fit(high, low, close,
                                              volume, 10)), 'MFI10')
        feat.add_feature(data_rank(mfi.fit(high, low, close,
                                              volume, 20)), 'MFI20')
        feat.add_feature(data_rank(mfi.fit(high, low, close,
                                              volume, 60)), 'MFI60')

        feat.add_feature(data_rank(rsi.fit(close, 10)), 'RSI10')
        feat.add_feature(data_rank(rsi.fit(close, 20)), 'RSI20')
        feat.add_feature(data_rank(rsi.fit(close, 60)), 'RSI60')

        feat.add_feature(data_rank(vol.fit(close, 10)), 'VOL10')
        feat.add_feature(data_rank(vol.fit(close, 20)), 'VOL20')
        feat.add_feature(data_rank(vol.fit(close, 60)), 'VOL60')

        feat.add_feature(data_rank(discount.fit(close, 63)),
                         'DISCOUNT63')
        feat.add_feature(data_rank(discount.fit(close, 126)),
                         'DISCOUNT126')

        # IBES
        discounts, discounts_smooth = make_ibes_discount(data)

        if live_flag:
            discounts = discounts.iloc[-1:]
            discounts.index = [0]
        feat.add_feature(data_rank(discounts), 'IBES_Discount')

        if live_flag:
            discounts_smooth = discounts_smooth.iloc[-1:]
            discounts_smooth.index = [0]
        feat.add_feature(data_rank(discounts_smooth), 'IBES_Discount_Smooth')

        increases, decreases = make_ibes_increases_decreases(data)

        if live_flag:
            increases = increases.iloc[-1:]
            increases.index = [0]
        feat.add_feature(data_rank(increases), 'IBES_Target_Increase')

        if live_flag:
            decreases = decreases.iloc[-1:]
            decreases.index = [0]
        feat.add_feature(data_rank(decreases), 'IBES_Target_Decrease')

        # Create output
        pdata = pd.DataFrame()
        pdata['SecCode'] = data.SecCode
        pdata['Date'] = data.Date
        pdata['TimeIndex'] = data.TimeIndex
        pdata['TestFlag'] = data.TestFlag

        n_id_features = pdata.shape[1]  # For capturing added features
        # Adjust date for faster live imp
        if live_flag:
            max_date = pdata.Date.max()
            pdata = pdata[pdata.Date == max_date].copy()
            pdata.Date = 0

        pdata = pdata.merge(feat.make_dataframe())

        # Extract features
        features = pdata.columns[n_id_features:].tolist()
        return pdata, features

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _initial_clean(self, data, time_index):
        data['TimeIndex'] = time_index
        data = create_split_multiplier(data)
        #data = clean_gsector_ggroup(data)
        return data

    def _make_earnings_data(self, data):
        # Earnings related fields
        data = data.merge(make_ern_date_blackout(data, start_ind=1, end_ind=5))
        data = data.merge(make_ern_return(data))
        data = data.merge(make_anchor_ret(data, init_offset=1, window=20))
        return data

    def _trim_to_one_quarter(self, data):
        # Trim only one quarter's worth of training data
        min_date = data.Date[data.TestFlag].min()
        trim_date = min_date - dt.timedelta(days=80)
        trim_date = dt.date(trim_date.year, trim_date.month, 1)
        data = data[data.Date >= trim_date].copy()
        return data

    def _get_pairs_info(self, data):
        pair_info = PairSelector().rank_pairs(
            data=data,
            filter_n_pairs_per_seccode=30)
        return pair_info

    def _get_pairs_z_score(self, data, pair_info, live_flag=False):
        _, zscores = PairSelector().get_z_scores(
            data=data,
            z_window=20,
            pair_info=pair_info,
            implementation=live_flag)
        return zscores

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

def create_split_multiplier(data):
    # SPLITS: Instead of using the levels, use the CHANGE in levels.
    # This is necessary for the updating of positions and prices downstream
    data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) * \
        (data.SecCode == data.SecCode.shift(1)).astype(int) + 1
    del data['SplitFactor']
    return data


def clean_gsector_ggroup(data):
    # Fill Nan values
    data['GGROUP'] = data.groupby('SecCode')['GGROUP'].transform(
        lambda x: x.fillna(x.iloc[0]))
    ggroup['GSECTOR'] = ggroup.GGROUP.apply(lambda x: x[:2])
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def merge_live_pricing_data(data, live_pricing_data):
    # Add missing columns to live pricing data, and merge
    new_cols = list(set(data.columns) - set(live_pricing_data.columns))
    live_pricing_data = live_pricing_data.join(
        pd.DataFrame(columns=new_cols), how='left')
    live_pricing_data['Date'] = dt.datetime.utcnow().date()
    live_pricing_data['TestFlag'] = True
    return data.append(live_pricing_data)


def adjust_todays_prices(data):
    data.AdjVwap = np.where(data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
    data.RClose = np.where(data.RClose.isnull(), data.AdjClose, data.RClose)
    return data


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
    output_inc = target_changes.copy()
    output_inc[:] = 0
    output_dec = output_inc.copy()
    increases = (target_changes > 0).astype(int)
    decreases = (target_changes < 0).astype(int)
    for i in range(5):
        output_inc += (increases.shift(i) * (1 - i*.2)).fillna(0)
        output_dec += (decreases.shift(i) * (1 - i*.2)).fillna(0)
    return output_inc, output_dec


def make_ibes_discount(data):
    """
    NOTE: 1-day lag
    """
    data['IBES_Discount'] = winsorize(
        data.PTARGETUNADJ / data.RClose - 1, limits=(0.005, 0.005))
    discounts = data.pivot(index='Date',
                           columns='SecCode',
                           values='IBES_Discount').shift(1)
    discounts_smooth = discounts.rolling(4).mean()
    return discounts, discounts_smooth


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


def make_ern_date_blackout(data, start_ind=1, end_ind=4):
    ernflag = data.pivot(index='Date', columns='SecCode',
                         values='EARNINGSFLAG').fillna(0)
    output = ernflag.copy()
    output[:] = 0
    for i in range(start_ind, end_ind + 1):
        output += ernflag.shift(i).fillna(0)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'EARNINGS_Blackout']
    return output


def make_anchor_ret(data, init_offset=0, window=20):
    """
    Init offset is the anchor date relative to the earnings date. From there,
    it will look `window` days in advance and compare the close prices to the
    close price on the anchor date. Once beyond the window, it just resets
    to zero.

    Parameters
    ----------
    init_offset : int
        The index relative to the earnings date that represents the first
        anchor price
    window : int
        The maximum number of days to look back to create the anchor price
    """
    closes = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    earningsflag = data.pivot(index='Date', columns='SecCode',
                              values='EARNINGSFLAG').fillna(0)
    # Get window period anchor price
    init_anchor = earningsflag.shift(init_offset).fillna(0) * closes
    end_anchor = init_anchor.shift(init_offset+window).fillna(0) * -1
    anchor_price = (init_anchor + end_anchor).cumsum()
    output = (closes / anchor_price) - 1
    output = output.replace(np.inf, 0).replace(-np.inf, 0)
    output = output.unstack().reset_index().fillna(0)
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
