import pandas as pd
import datetime as dt

from statsmodels.tsa.stattools import adfuller

from ram.data.feature_creator import *

from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb.utils import make_arg_iter

from ram.strategy.statarb.version_001.data.pairs_selector import PairSelector


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

        if (time_index < 54):
            return

        data['TimeIndex'] = time_index
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) * \
            (data.SecCode == data.SecCode.shift(1)).astype(int) + 1

        dates = data[['Date', 'TestFlag']].drop_duplicates()
        train_dates = dates.Date[~dates.TestFlag]
        test_dates = dates.Date[dates.TestFlag]

        # Filter out small stocks
        filter_ = data[data.Date == max(train_dates)]
        filter_ = filter_[filter_.AvgDolVol > 5]
        data = data[data.SecCode.isin(filter_.SecCode)]

        ## PORTFOLIO PAIRS
        close_ = clean_pivot_raw_data(data, 'AdjClose')

        # Fit random portfolios
        unique_seccodes = close_.columns

        indexes = np.log(close_).diff().fillna(0).values

        # Generate random, unique indexes
        SAMPLES = 10000  # Change to 10000
        X = np.random.randint(0, high=len(unique_seccodes), size=(20000, 4))
        X1 = X.copy()
        X1.sort(axis=1)
        inds = np.sum(np.diff(X1, axis=1) == 0, axis=1)
        X = X[inds == 0]
        X = X[:SAMPLES]

        index1 = pd.DataFrame((indexes[:, X[:, 0]] + indexes[:, X[:, 1]]) / 2., index=close_.index).cumsum()
        index2 = pd.DataFrame((indexes[:, X[:, 2]] + indexes[:, X[:, 3]]) / 2., index=close_.index).cumsum()

        index3 = (index1 - index2)

        zscore = (index3 - index3.rolling(20).mean()) / index3.rolling(20).std()
        seccodes = np.take(unique_seccodes, X)
        zscore.columns = ['{}_{}~{}_{}'.format(*x) for x in seccodes]

        index4 = index3.loc[train_dates]

        # scores = []
        # for i in range(SAMPLES):
        #     scores.append(adfuller(index4.iloc[:, i])[0])

        scores = (np.sign(index4).diff() != 0).sum().values * -1

        # Keep top 200 portfolios
        inds = np.argsort(scores)

        zscore_c = zscore.iloc[:, inds[:200]]

        # Create output
        pdata = pd.DataFrame()
        pdata['SecCode'] = data.SecCode
        pdata['Date'] = data.Date
        pdata['TimeIndex'] = data.TimeIndex
        pdata['TestFlag'] = data.TestFlag
        pdata['AvgDolVol'] = data.AvgDolVol
        pdata['MarketCap'] = data.MarketCap
        pdata['RClose'] = data.RClose
        pdata['RCashDividend'] = data.RCashDividend
        pdata['SplitMultiplier'] = data.SplitMultiplier

        # Make test data
        #pdata = pdata[pdata.TestFlag].reset_index(drop=True)

        # Preprocessing test data
        pdata['keep_inds'] = (pdata.AvgDolVol >= LOW_LIQUIDITY_FILTER) & \
            (pdata.RClose >= LOW_PRICE_FILTER)

        trade_data = {}
        trade_data['closes'] = make_variable_dict(pdata, 'RClose')
        trade_data['dividends'] = make_variable_dict(pdata, 'RCashDividend', 0)
        trade_data['splits'] = make_variable_dict(pdata, 'SplitMultiplier', 1)
        trade_data['liquidity'] = make_variable_dict(pdata, 'AvgDolVol')

        trade_data['test_dates'] = test_dates

        trade_data['pair_data'] = {
            'zscores': zscore_c
        }
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
