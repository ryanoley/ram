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

        feat = FeatureAggregator()

        # Daily returns
        feat.add_feature(close / open_ - 1, 'day_ret')
        feat.add_feature(open_ / close.shift(1) - 1, 'close_to_open_ret')
        feat.add_feature(close.pct_change(), 'overnight_ret')

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

        pdata = pdata.merge(feat.make_dataframe())

        # Make test data
        pdata = pdata[pdata.TestFlag].reset_index(drop=True)

        # Preprocessing test data
        keep_inds = (pdata.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (pdata.RClose >= LOW_PRICE_FILTER)

        trade_data = {}

        trade_data['closes'] = make_variable_dict(pdata, 'RClose')
        trade_data['dividends'] = make_variable_dict(pdata, 'RCashDividend', 0)
        trade_data['splits'] = make_variable_dict(pdata, 'SplitMultiplier', 1)
        trade_data['liquidity'] = make_variable_dict(pdata, 'AvgDolVol')

        trade_data['day_ret'] = rank_filter_data(
            pdata, 'day_ret', keep_inds, False)
        trade_data['overnight_ret'] = rank_filter_data(
            pdata, 'overnight_ret', keep_inds, False)

        self.trade_data = trade_data


def rank_filter_data(data, value_column, keep_index, rank=True):
    data = data[['SecCode', 'Date', value_column]].copy()
    data.loc[~keep_index, value_column] = np.nan
    data = data.pivot(index='Date', columns='SecCode',
                      values=value_column)
    if rank:
        return data.rank(axis=1, pct=True)
    else:
        return data
