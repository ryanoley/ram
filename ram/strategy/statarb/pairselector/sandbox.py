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


strategy = StatArbStrategy('version_0005', False)
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

i = 14

data, features = get_data(pairselector, i)

train_dates = data.Date[~data.TestFlag].drop_duplicates().values
test_dates = data.Date[data.TestFlag].drop_duplicates().values

# Test dates for only one quarter forward
qtrs = np.array([(x.month-1)/3 + 1 for x in test_dates])
test_dates = test_dates[qtrs == qtrs[0]]



# ~~~~~~ Responses ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ram.strategy.statarb.responses.response1 import response_strategy_1


pairselector.rank_pairs(data, 50)

# Get train close prices for response creation
close1 = pairselector.close_data.loc[train_dates][pairselector.pair_info.Leg1]
close2 = pairselector.close_data.loc[train_dates][pairselector.pair_info.Leg2]
resp1 = close1.copy()
resp2 = close2.copy()

resp1[:], resp2[:] = response_strategy_1(close1.values, close2.values, 0.02, 6)
# Rename columns
resp1.columns = range(resp1.shape[1])
resp2.columns = range(resp1.shape[1], resp1.shape[1]+resp2.shape[1])

zscores = pairselector.get_zscores(40)

X = zscores.loc[train_dates].iloc[40:]
y = resp1.join(resp2).iloc[:-6]

dates = list(set(X.index).intersection(y.index))


clf = RandomForestClassifier()

clf.fit(X=X.loc[dates], y=(y.loc[dates] > 0))


## Out of sample

preds = clf.predict(zscores.loc[test_dates])

y.loc[dates].values[~preds.astype(bool)].mean()

close1_test = pairselector.close_data.loc[test_dates][pairselector.pair_info.Leg1]
close2_test = pairselector.close_data.loc[test_dates][pairselector.pair_info.Leg2]
resp1_test = close1_test.copy()
resp2_test = close2_test.copy()

resp1_test[:], resp2_test[:] = response_strategy_1(close1_test.values, close2_test.values, 0.02, 6)


resp1_test.columns = range(resp1_test.shape[1])
resp2_test.columns = range(resp1_test.shape[1], resp1_test.shape[1]+resp2_test.shape[1])

resp1_test.join(resp2_test).values[preds == 1]







