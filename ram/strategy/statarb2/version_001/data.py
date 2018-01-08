import pandas as pd
import datetime as dt

from ram.data.feature_creator import *

from ram.strategy.statarb.utils import make_arg_iter


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
        feat.add_feature(data_rank(close / open_), 'day_ret')

        # PRMA vals
        prma = PRMA()
        for i in [5, 10, 15, 20]:
            feat.add_feature(data_rank(prma.fit(close, i)),
                             'prma_{}'.format(i))

        # Create output
        pdata = pd.DataFrame()
        pdata['SecCode'] = data.SecCode
        pdata['Date'] = data.Date
        pdata['TimeIndex'] = data.TimeIndex
        pdata['TestFlag'] = data.TestFlag
        pdata['MarketCap'] = data.MarketCap
        pdata['AvgDolVol'] = data.AvgDolVol
        pdata['RClose'] = data.RClose
        pdata['RCashDividend'] = data.RCashDividend
        pdata['SplitMultiplier'] = data.SplitMultiplier

        pdata = pdata.merge(feat.make_dataframe())

        # Trim to one quarter
        max_train_date = pdata.Date[~data.TestFlag].max()
        trim_date = max_train_date - dt.timedelta(days=80)
        trim_date = dt.date(trim_date.year, trim_date.month, 1)
        pdata = pdata[pdata.Date >= trim_date].copy()

        # Set training and test data
        self.train_data = pdata[~pdata.TestFlag].reset_index(drop=True)
        self.test_data = pdata[pdata.TestFlag].reset_index(drop=True)
