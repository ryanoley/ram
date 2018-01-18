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

        # PRMA vals
        prma = PRMA()
        for i in [5, 10, 15, 20]:
            feat.add_feature(prma.fit(close, i), 'prma_{}'.format(i))


        # Original
        temp = -1 * prma.fit(close, 3) / prma.fit(close, 10)
        feat.add_feature(temp, 'mom_3_10')

        feat.add_feature(prma.fit(close, 10) / prma.fit(close, 2), 'prma_2_10')
        feat.add_feature(prma.fit(close, 10) / prma.fit(close, 3), 'prma_3_10')
        feat.add_feature(prma.fit(close, 20) / prma.fit(close, 4), 'prma_4_20')
        feat.add_feature(prma.fit(close, 30) / prma.fit(close, 5), 'prma_5_30')

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

        pdata = pdata.merge(feat.make_dataframe())
        # Make test data
        pdata = pdata[pdata.TestFlag].reset_index(drop=True)

        # Trim to one quarter
        #max_train_date = pdata.Date[~data.TestFlag].max()
        #trim_date = max_train_date - dt.timedelta(days=80)
        #trim_date = dt.date(trim_date.year, trim_date.month, 1)
        #pdata = pdata[pdata.Date >= trim_date].copy()

        # Preprocessing test data
        keep_inds = (pdata.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (pdata.RClose >= LOW_PRICE_FILTER)

        trade_data = {}
        trade_data['prma_2_10'] = rank_filter_data(pdata, 'prma_2_10', keep_inds)
        trade_data['prma_3_10'] = rank_filter_data(pdata, 'prma_3_10', keep_inds)
        trade_data['prma_4_20'] = rank_filter_data(pdata, 'prma_4_20', keep_inds)
        trade_data['prma_5_30'] = rank_filter_data(pdata, 'prma_5_30', keep_inds)

        trade_data['prma_5'] = rank_filter_data(pdata, 'prma_5', keep_inds)
        trade_data['prma_10'] = rank_filter_data(pdata, 'prma_10', keep_inds)
        trade_data['prma_15'] = rank_filter_data(pdata, 'prma_15', keep_inds)
        trade_data['prma_20'] = rank_filter_data(pdata, 'prma_20', keep_inds)

        trade_data['closes'] = make_variable_dict(pdata, 'RClose')
        trade_data['dividends'] = make_variable_dict(pdata, 'RCashDividend', 0)
        trade_data['splits'] = make_variable_dict(pdata, 'SplitMultiplier', 1)
        trade_data['liquidity'] = make_variable_dict(pdata, 'AvgDolVol')

        trade_data['day_ret_rank'] = rank_filter_data(pdata, 'day_ret', keep_inds)
        trade_data['day_ret_abs'] = rank_filter_data(pdata, 'day_ret', keep_inds, False)

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
