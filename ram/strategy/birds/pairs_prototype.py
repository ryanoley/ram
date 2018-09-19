import pandas as pd
import matplotlib.pyplot as plt

from ram.strategy.birds.main import BirdsStrategy
from ram.strategy.birds.data import get_features
from ram.strategy.birds.pairs import Pairs

from sklearn.ensemble import ExtraTreesClassifier

from ram.data.feature_creator import *


strategy = BirdsStrategy(strategy_code_version='version_0001',
                         prepped_data_version='version_0027')

strategy._get_prepped_data_file_names()

market_data = strategy.read_market_index_data()


features = pd.DataFrame()
# for i in range(150, len(strategy._prepped_data_files)):
for i in range(150, 160):
    data = strategy.read_data_from_index(i)
    f = get_features(data, n_groups=5, n_days=3)
    f['tindex'] = i
    features = features.append(f)
    print(i)



# Pairs trading

features2 = features[features.DailyReturn.notnull()]
features2 = features2.pivot(index='Date',
                            columns='Group',
                            values='DailyReturn')


# Stack a bunch

pairs = Pairs().get_best_pairs(features2,
                               features2.index[-20],
                               z_window=20,
                               max_pairs=100000)





feature = 'BOLL280_1_PRMA20_2'

zscore = pairs[1][feature]
rets = pairs[0][feature]

out = rets.copy()
out[:] = 0

side = 0
entry_thresh = 1.5
exit_thresh = 1

for d in out.index:

    out.loc[d] = rets.loc[d] * side

    if side == 0:
        if zscore.loc[d] > entry_thresh:
            side = -1
        elif zscore.loc[d] < -entry_thresh:
            side = 1

    elif side == 1:
        if zscore.loc[d] > -exit_thresh:
            side = 0

    elif side == -1:
        if zscore.loc[d] < exit_thresh:
            side = 0






