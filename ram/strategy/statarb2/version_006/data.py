import pandas as pd
import datetime as dt

from ram.data.feature_creator import *

from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb.utils import make_arg_iter

LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3


class DataContainer(object):

    def __init__(self):
        self.train_data = pd.DataFrame()
        self.test_data = pd.DataFrame()
        self._response_days_args = [5, 10]

    def get_args(self):
        return make_arg_iter({
            'response_days': self._response_days_args,
        })

    def set_args(self, response_days):
        self._response_days = response_days
        self.train_data['Response'] = self.train_data['Response_{}'.format(response_days)]
        # Add training flag given number of response days
        training_dates = self.train_data.Date.unique()
        training_dates = training_dates[:-self._response_days]
        self.train_data['TrainFlag'] = self.train_data.Date.isin(training_dates)

    def process_training_data(self, data, market_data, time_index):

        data['TimeIndex'] = time_index
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) * \
            (data.SecCode == data.SecCode.shift(1)).astype(int) + 1

        open_ = clean_pivot_raw_data(data, 'AdjOpen')
        close = clean_pivot_raw_data(data, 'AdjClose')

        feat = FeatureAggregator()

        # Daily returns
        feat.add_feature(data_rank(close / open_), 'day_ret')

        # PRMA vals
        prma = PRMA()
        for i in [5, 10, 15, 20]:
            feat.add_feature(data_rank(prma.fit(close, i)), 'prma_{}'.format(i))

        # Smoothed prma
        feat.add_feature(data_rank(prma.fit(close, 10) / prma.fit(close, 2)), 'prma_2_10')
        feat.add_feature(data_rank(prma.fit(close, 10) / prma.fit(close, 3)), 'prma_3_10')
        feat.add_feature(data_rank(prma.fit(close, 20) / prma.fit(close, 4)), 'prma_4_20')
        feat.add_feature(data_rank(prma.fit(close, 30) / prma.fit(close, 5)), 'prma_5_30')

        # LONG returns
        feat.add_feature(data_rank(close.pct_change(10)), 'ret_10d')
        feat.add_feature(data_rank(close.pct_change(20)), 'ret_20d')
        feat.add_feature(data_rank(close.pct_change(40)), 'ret_40d')

        # Accounting features
        pdata2 = self._make_features(data)

        # Respones
        responses = self._make_responses(data)

        # Create output
        pdata = pd.DataFrame()
        pdata['SecCode'] = data.SecCode
        pdata['Date'] = data.Date
        pdata['TimeIndex'] = data.TimeIndex
        pdata['TestFlag'] = data.TestFlag
        pdata['AvgDolVol'] = data.AvgDolVol
        pdata['RClose'] = data.RClose
        pdata['RCashDividend'] = data.RCashDividend
        pdata['SplitMultiplier'] = data.SplitMultiplier
        pdata['keep_inds'] = (pdata.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (pdata.RClose >= LOW_PRICE_FILTER)

        feature_ind = pdata.shape[1]
        pdata = pdata.merge(feat.make_dataframe())
        pdata = pdata.merge(pdata2)
        self.features = pdata.columns[feature_ind:]

        pdata = pdata.merge(responses)

        # Get one month's data for training data
        train = pdata[~pdata.TestFlag]
        max_date = train.Date.max()
        train = train[train.Date.apply(lambda x: x.month) == max_date.month]
        test = pdata[pdata.TestFlag]

        self.train_data = self.train_data.append(train)
        self.test_data = test

        keep_inds = (test.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (test.RClose >= LOW_PRICE_FILTER)

        # Capture test trading data
        trade_data = {}

        trade_data['closes'] = make_variable_dict(test, 'RClose')
        trade_data['dividends'] = make_variable_dict(test, 'RCashDividend', 0)
        trade_data['splits'] = make_variable_dict(test, 'SplitMultiplier', 1)
        trade_data['liquidity'] = make_variable_dict(test, 'AvgDolVol')

        trade_data['prma_5'] = rank_filter_data(test, 'prma_5', keep_inds)
        trade_data['prma_10'] = rank_filter_data(test, 'prma_10', keep_inds)
        trade_data['prma_15'] = rank_filter_data(test, 'prma_15', keep_inds)
        trade_data['prma_20'] = rank_filter_data(test, 'prma_20', keep_inds)

        self.trade_data = trade_data

    def _make_features(self, data):
        """
        Everything but Technical features. Separated for speed during
        implementation.
        """
        feat = FeatureAggregator()

        for feature in accounting_features:
            temp = clean_pivot_raw_data(data, feature, lag=1)
            feat.add_feature(data_rank(temp), feature)

        for feature in starmine_features:
            temp = clean_pivot_raw_data(data, feature, lag=1)
            feat.add_feature(data_rank(temp), feature)

        pdata = data[['SecCode', 'Date']].copy()
        pdata = pdata.merge(feat.make_dataframe())

        # Manually handle SI Data
        si_vars = ['SIINSTOWNERSHIP', 'SIMARKETCAPRANK', 'SIRANK',
                   'SISECTORRANK', 'SISHORTSQUEEZE', 'SIUNADJRANK']
        pdata.loc[:, si_vars] = pdata.loc[:, si_vars].fillna(0.5)
        return pdata

    def _make_responses(self, data):
        for i, days in enumerate(self._response_days_args):
            temp = simple_responses(data, days=days)
            temp.columns = ['SecCode', 'Date', 'Response_{}'.format(days)]
            if i == 0:
                responses = temp
            else:
                responses = responses.merge(temp)
        return responses


def simple_responses(data, days=2):
    """
    Just return 1 or 0 for Position or Negative return
    """
    assert isinstance(days, int)
    rets = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    rets2 = (rets.pct_change(days).shift(-days).rank(axis=1, pct=True) >= 0.5).astype(int)
    output = rets2.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response']
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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
