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



###############################################################################
#  Mean responses by variable top for top vs bottom

# Search feature responses
features['group_1'] = features.Group.apply(lambda x: x.split('_')[0])

out = pd.DataFrame()

for i, g in enumerate(features.group_1.unique()):
    out.loc[i, 'Grp'] = g
    out.loc[i, 'Top'] = features[features.Group == '{}_1'.format(g)].Response.mean()
    out.loc[i, 'Bottom'] = features[features.Group == '{}_5'.format(g)].Response.mean()

out['Diff'] = (out.Top - out.Bottom).abs()

out.sort_values('Diff')



###############################################################################
# Top vs Bottom returns

# Create indexes on returns, thus drop nan DailyReturn
features2 = features[features.DailyReturn.notnull()]
features2 = features2.pivot(index='Date',
                            columns='Group',
                            values='DailyReturn')


out2 = pd.DataFrame()

for i, g in enumerate(features.group_1.unique()):
    out2.loc[:, g] = features2['{}_5'.format(g)] - features2['{}_1'.format(g)]


plt.figure()
plt.plot(out2.cumsum())
plt.show()




