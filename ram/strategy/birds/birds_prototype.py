import pandas as pd
import matplotlib.pyplot as plt

from ram.strategy.birds.main import BirdsStrategy
from ram.strategy.birds.data import get_features
from ram.strategy.birds.pairs import Pairs

from sklearn import metrics
from sklearn.grid_search import GridSearchCV
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import GradientBoostingClassifier

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
#  Responses across all groups, not just intragroup

returns = features[features.DailyReturn.notnull()].pivot(
    index='Date', columns='Group', values='DailyReturn')

ndays = 4

responses = returns.rolling(window=ndays).sum().shift(-ndays).dropna()

responses = (responses.rank(pct=True, axis=1) > 0.5).astype(int)

responses = responses.unstack().reset_index()
responses.columns = ['Group', 'Date', 'Response']

features2 = features.merge(responses, how='left')


###############################################################################
#  Make features on top of daily return series

returns = features[features.DailyReturn.notnull()].pivot(
    index='Date', columns='Group', values='DailyReturn')

group_indexes = (returns + 1).cumprod()

# PRMA
ma = group_indexes.rolling(window=10).mean()
prma = group_indexes / ma
prma = prma.unstack().reset_index()
prma.columns = ['Group', 'Date', 'prma']

# DISCOUNT
disc = group_indexes / group_indexes.cummax() - 1
disc = disc.unstack().reset_index()
disc.columns = ['Group', 'Date', 'disc']

# VOL
vol = returns.rolling(window=10).std()
vol = vol.unstack().reset_index()
vol.columns = ['Group', 'Date', 'vol']

# LAGGED and SMOOTHED returns
rets = returns.copy()
rets = rets.unstack().reset_index()
rets.columns = ['Group', 'Date', 'rets']

rets2 = returns.rolling(window=2).sum()
rets2 = rets2.unstack().reset_index()
rets2.columns = ['Group', 'Date', 'rets2']

# ALL Groups
all_groups = returns.copy()
all_groups.columns = ['grp_{}'.format(x) for x in all_groups.columns]
grp_features = all_groups.columns.tolist()
all_groups = all_groups.reset_index()

# MERGE
data = responses.merge(prma).merge(disc).merge(vol).merge(rets).merge(rets2).merge(all_groups)
data = data.dropna()

all_dates = data.Date.unique()
train_dates = all_dates[:-15]
test_dates = all_dates[-15:]

train_data = data[data.Date.isin(train_dates)]
test_data = data[data.Date.isin(test_dates)]

all_features = ['prma', 'disc', 'vol', 'rets', 'rets2'] + grp_features

X_train = train_data[all_features]
y_train = train_data['Response']

X_test = test_data[all_features]
y_test = test_data['Response']


###############################################################################
# GRID SEARCH

tree = ExtraTreesClassifier()

parameters = {
    'n_estimators': [10, 100],
    'min_samples_leaf': [30, 100, 1000],
    'criterion': ['gini', 'entropy'],
    'max_features': ['auto' ,'sqrt', 'log2'],
    'max_depth': [4, 10, 20, 40, 80]
}

clf = GridSearchCV(tree, parameters, n_jobs=3, verbose=2)
clf.fit(X=X_train, y=y_train)


print(clf.best_params_)


metrics.accuracy_score(y_train, clf.predict(X_train))
metrics.accuracy_score(y_test, clf.predict(X_test))



boost = GradientBoostingClassifier()

parameters = {
    'n_estimators': [10],
    'min_samples_leaf': [30, 100, 1000],
    'max_depth': [4, 10, 20, 40, 80],
    'max_features': ['auto' ,'sqrt', 'log2'],
    'loss': ['deviance', 'exponential'],
    'learning_rate': [0.01, 0.1, 1.0]
}

clf2 = GridSearchCV(boost, parameters, n_jobs=3, verbose=2)
clf2.fit(X=X_train, y=y_train)


print(clf.best_params_)


metrics.accuracy_score(y_train, clf.predict(X_train))
metrics.accuracy_score(y_test, clf.predict(X_test))








