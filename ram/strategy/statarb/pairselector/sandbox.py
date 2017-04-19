import os
import itertools
import numpy as np
import pandas as pd

from ram.strategy.statarb.main import StatArbStrategy
from ram.strategy.statarb.pairselector.pairs2 import PairSelector2
from ram.strategy.statarb.pairselector.pairs2 import get_return_series
from ram.strategy.statarb.pairselector.pairs2 import get_trade_signal_series

from gearbox import find_quantiles
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression


strategy = StatArbStrategy('version_0005', False)

strategy._prepped_data_dir = '/Users/mitchellsuter/Desktop/version_0005'
strategy._get_data_file_names()

pairselector = PairSelector2()


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



for i in range(len(strategy._data_files)):

    data, features = get_data(pairselector, i)

    train_dates = data.Date[~data.TestFlag].drop_duplicates().values
    test_dates = data.Date[data.TestFlag].drop_duplicates().values
    # Test dates for only one quarter forward
    qtrs = np.array([(x.month-1)/3 + 1 for x in test_dates])
    test_dates = test_dates[qtrs == qtrs[0]]

    # ~~~ PREDICTIVE DATA SET ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #  Big flaw is that responses can bleed into test data
    pairselector.rank_pairs(data)

    responsesL, responsesS = pairselector.get_responses2(0.06, 30)
    zscores = pairselector.get_zscores(20)
    break



import matplotlib.pyplot as plt


plt.figure()
plt.plot(responsesL.mean(axis=1))
plt.plot(responsesS.mean(axis=1))
plt.show()





if True:
    # Unstack zscores and responses
    responses2 = responsesL.unstack().reset_index()
    responses2.columns = ['Pair', 'Date', 'Response']

    zscores2 = zscores.unstack().reset_index()
    zscores2.columns = ['Pair', 'Date', 'ZScore']
    zscores2['ZScoreLag1'] = zscores2.ZScore.shift(1)
    zscores2['ZScoreLag2'] = zscores2.ZScore.shift(2)
    zscores2['ZScoreLag3'] = zscores2.ZScore.shift(3)

    df = responses2.merge(zscores2).dropna()

    # Merge data
    df['SecCode'] = df.Pair.apply(lambda x: x.split('~')[0])
    df = df.merge(data[['SecCode', 'Date']+features])

    df['SecCode'] = df.Pair.apply(lambda x: x.split('~')[1])
    df = df.merge(data[['SecCode', 'Date']+features],
                  left_on=['SecCode', 'Date'],
                  right_on=['SecCode', 'Date'], suffixes=('_leg1', '_leg2'))

    train_features = [f + '_leg1' for f in features] + \
        [f + '_leg2' for f in features]
    train_features += ['ZScore', 'ZScoreLag1', 'ZScoreLag2', 'ZScoreLag3']

    train_df = df[df.Date.isin(train_dates)].copy()
    test_df = df[df.Date.isin(test_dates)].copy()

    #rfc = RandomForestClassifier(n_estimators=8,
    #                             min_samples_leaf=100,
    #                             n_jobs=-1,
    #                             verbose=2)

    rfc = LogisticRegression()
    rfc.fit(X=train_df[train_features], y=train_df.Response)

    test_df['preds'] = rfc.predict_proba(test_df[train_features])[:, 1]

    testout = pd.DataFrame(columns=['Top', 'Bottom'], index=test_dates)

    for d in test_dates:
        zz = test_df[test_df.Date == d].copy()
        zz = zz.sort_values('preds')
        cc = len(zz) / 4
        testout.loc[d, 'Bottom'] = zz.Response.iloc[:cc].mean()
        testout.loc[d, 'Top'] = zz.Response.iloc[-cc:].mean()

    if i == 0:
        out = pd.DataFrame(columns=['Top', 'Bottom'])
    out.loc[i] = testout.mean()
    print i


import matplotlib.pyplot as plt

z = test_df[['Date', 'preds']].set_index('Date')


plt.figure()
plt.plot(testout)
plt.show()





