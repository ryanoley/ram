import os
import itertools
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

from ram.strategy.statarb.main import StatArbStrategy
from ram.strategy.statarb.pairselector.pairs2 import PairSelector2

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression


strategy = StatArbStrategy('version_0017', False)
strategy._get_data_file_names()
pairselector = PairSelector2()



# ~~~~~~ MAKE FEATURES ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_data(pairselector, index=3):

    data = strategy.read_data_from_index(index)

    # Formatting of data
    data['PRMA30_AvgDolVol'] = (data.AdjClose * data.AdjVolume / 1e6) / \
        data.AvgDolVol

    data['VOLRatio'] = data.VOL5_AdjClose / data.VOL20_AdjClose

    features = ['PRMA30_AvgDolVol', 'VOLRatio', 'PRMA10_AdjClose']

    # Normalize features by day
    means_ = data.groupby('Date')[features].mean()
    means_.columns = [f + '_mean' for f in features]
    means_ = means_.reset_index()
    stds_ = data.groupby('Date')[features].std()
    stds_.columns = [f + '_std' for f in features]
    stds_ = stds_.reset_index()

    data = data.merge(means_).merge(stds_)

    for f in features:
        data[f] = (data[f] - data[f+'_mean']) / data[f+'_std']
        data = data.drop([f+'_mean', f+'_std'], axis=1)

    return data[['SecCode', 'Date', 'AdjClose', 'TestFlag']+features], features



# ~~~~~~ Import some data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

i = 2

data, features = get_data(pairselector, i)

train_dates = data.Date[~data.TestFlag].drop_duplicates().values
test_dates = data.Date[data.TestFlag].drop_duplicates().values

# Test dates for only one quarter forward
qtrs = np.array([(x.month-1)/3 + 1 for x in test_dates])
test_dates = test_dates[qtrs == qtrs[0]]



# ~~~~~~ Responses ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ram.strategy.statarb.responses.response1 import response_strategy_1


pairselector.rank_pairs(data, 500)

# Get train close prices for response creation
close1 = pairselector.close_data.loc[train_dates][pairselector.pair_info.Leg1]
close2 = pairselector.close_data.loc[train_dates][pairselector.pair_info.Leg2]

zscores = pairselector.get_zscores(40)


resp1, resp2 = response_strategy_1(close1, close2, 0.05, 6)



# ~~~~~~ How do responses look when major move? ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


inds = close1.pct_change().fillna(0).values - close2.pct_change().fillna(0).values < -0.05

out = pd.DataFrame(columns=['Mean1', 'Mean2', 'Count1', 'Count2'])

for i in range(inds.shape[1]):
    rets1 = resp1.iloc[:, i][inds[:, i]].dropna()
    rets2 = resp2.iloc[:, i][inds[:, i]].dropna()
    rets1 = rets1[dt.date(2009, 1, 1):]
    rets2 = rets2[dt.date(2009, 1, 1):]
    out.loc[i, 'Mean1'] = rets1.mean()
    out.loc[i, 'Mean2'] = rets2.mean()
    out.loc[i, 'Count1'] = len(rets1)
    out.loc[i, 'Count2'] = len(rets2)







