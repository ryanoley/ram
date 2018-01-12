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

    def get_args(self):
        return make_arg_iter({
            'data1': [1],
        })

    def set_args(self, data1):
        pass

    def process_training_data(self, data, market_data, time_index):

        data['TimeIndex'] = time_index
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) * \
            (data.SecCode == data.SecCode.shift(1)).astype(int) + 1

        open_ = clean_pivot_raw_data(data, 'AdjOpen')
        close = clean_pivot_raw_data(data, 'AdjClose')
        rets = close.pct_change().iloc[1:]

        feat = FeatureAggregator()

        # Make response
        ranks = rets.shift(-1).rank(axis=1, pct=True)
        response = ranks.copy()
        response[:] = 0
        response[ranks > 0.55] = 1
        response[ranks < 0.45] = -1
        feat.add_feature(response, 'Response')

        # Daily returns
        feat.add_feature(close / open_ - 1, 'day_ret')

        # Lag rets
        for i in range(0, 6):
            feat.add_feature(rets.shift(i), 'lag_{}_ret'.format(i))

        prma = PRMA()
        for i in [5, 10, 15, 20]:
            feat.add_feature(prma.fit(close, i), 'prma_{}'.format(i))

        # Create output
        pdata = pd.DataFrame()
        pdata['SecCode'] = data.SecCode
        pdata['Date'] = data.Date
        pdata['TimeIndex'] = data.TimeIndex
        pdata['TestFlag'] = data.TestFlag
        pdata['AvgDolVol'] = data.AvgDolVol
        pdata['RClose'] = data.RClose
        pdata['AdjClose'] = data.AdjClose
        pdata['RCashDividend'] = data.RCashDividend
        pdata['SplitMultiplier'] = data.SplitMultiplier
        pdata['keep_inds'] = (pdata.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (pdata.RClose >= LOW_PRICE_FILTER)

        pdata = pdata.merge(feat.make_dataframe())

        # Make train/test data
        train = pdata[~pdata.TestFlag].reset_index(drop=True)
        test = pdata[pdata.TestFlag].reset_index(drop=True)

        # Trim to one month for training data
        max_month = max(train.Date).month
        def max_month_flag(x, month):
            return x.month == max_month
        train = train[train.Date.apply(max_month_flag, args=(max_month,))]

        trade_data = {}

        trade_data['closes'] = make_variable_dict(test, 'RClose')
        trade_data['dividends'] = make_variable_dict(test, 'RCashDividend', 0)
        trade_data['splits'] = make_variable_dict(test, 'SplitMultiplier', 1)
        trade_data['liquidity'] = make_variable_dict(test, 'AvgDolVol')

        self.trade_data = trade_data
        self.train_data = self.train_data.append(train)
        self.test_data = test


def rank_filter_data(data, value_column, keep_index, rank=True):
    data = data[['SecCode', 'Date', value_column]].copy()
    data.loc[~keep_index, value_column] = np.nan
    data = data.pivot(index='Date', columns='SecCode',
                      values=value_column)
    if rank:
        return data.rank(axis=1, pct=True)
    else:
        return data
