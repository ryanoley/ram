import itertools
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from ram.strategy.statarb.main import StatArbStrategy
from ram.strategy.statarb.pairselector.pairs2 import PairSelector2
from ram.strategy.statarb.pairselector.pairs2 import get_return_series
from ram.strategy.statarb.pairselector.pairs2 import get_trade_signal_series


strategy = StatArbStrategy('version_0004', False)

pairselector = PairSelector2()

data = strategy.read_data_from_index(14)

params = {
    'pair_test_flag': True
}



# ~~~ PREDICTIVE DATA SET ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Big flaw is that responses can bleed into test data

pairselector.rank_pairs(data, **params)

responses = pairselector.get_responses(z_window=20, enter_z=2, exit_z=0)
zscores = pairselector.get_zscores(20)

# Unstack zscores and responses

responses2 = responses.unstack().reset_index()
responses2.columns = ['Pair', 'Date', 'Response']

zscores2 = zscores.unstack().reset_index()
zscores2.columns = ['Pair', 'Date', 'ZScore']
















"""
















z_windows = [20, 25, 30, 35, 40, 45, 50, 55, 60, 65]
enter_zs = [1.5, 1.6, 1.7, 1.8, 1.9, 2, 2.1, 2.2, 2.3, 2.5, 2.6]
exit_zs = [-0.2, -0.1, 0, 0.1, 0.2, 0.3, 0.4, 0.5]

close1 = close_data[pair_info.Leg1].values
close2 = close_data[pair_info.Leg2].values

best_z_window = np.zeros(close1.shape[1])
best_entry = np.zeros(close1.shape[1])
best_exit = np.zeros(close1.shape[1])
best_returns_train = np.zeros(close1.shape[1])
best_returns_test = np.zeros(close1.shape[1])
best_z_scores = np.zeros(close1.shape)

train_dates = data.Date[~data.TestFlag].unique()
train_inds = close_data.index.isin(train_dates)
test_inds = ~train_inds

for z_window in z_windows:

    zscores = _get_zscores(close_data, pair_info,
                           z_window=z_window).fillna(0).values

    for enter_z, exit_z in itertools.product(enter_zs, exit_zs):

        returns = np.zeros(close1.shape)
        counts = np.zeros(close1.shape[1])
        get_return_series(enter_z, exit_z, zscores, close1,
                          close2, returns, counts)

        returns_train = returns[train_inds].sum(axis=0)
        returns_test = returns[test_inds].sum(axis=0)

        eval_inds = returns_train > best_returns_train
        best_z_scores = np.where(eval_inds, zscores, best_z_scores)
        best_z_window = np.where(eval_inds, z_window, best_z_window)
        best_entry = np.where(eval_inds, enter_z, best_entry)
        best_exit = np.where(eval_inds, exit_z, best_exit)
        best_returns_test = np.where(eval_inds, returns_test, best_returns_test)
        best_returns_train = np.where(eval_inds, returns_train, best_returns_train)

    print z_window, np.mean(returns_train), pd.Series(returns_test).dropna().mean()







"""