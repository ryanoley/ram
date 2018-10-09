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


for i in range(150, len(strategy._prepped_data_files)):
    data = strategy.read_data_from_index(i)
    f = get_features(data, n_groups=5, n_days=3)
    f['tindex'] = i
    features = features.append(f)
    print(i)


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

# One-day and SMOOTHED returns
rets = returns.copy()
rets = rets.unstack().reset_index()
rets.columns = ['Group', 'Date', 'rets_1day']

rets_5day = returns.rolling(window=5).sum()
rets_5day = rets_5day.unstack().reset_index()
rets_5day.columns = ['Group', 'Date', 'rets_5day']

# Spreads on rolling returns
spread_rets = returns.rolling(window=10).sum()

group_vars = list(set([x.split('_')[0] for x in returns.columns]))
n_groups = max(set([x.split('_')[1] for x in returns.columns]))

spreads = pd.DataFrame()
for v in group_vars:
    spreads[v + '_ret_spread'] = spread_rets[v+'_'+n_groups] - spread_rets[v+'_1']
spreads = spreads.reset_index()


# MERGE
responses = features[['Group', 'Date', 'tindex', 'UnivResponse']]
data = responses.merge(prma).merge(disc).merge(vol).merge(rets).merge(rets_5day).merge(spreads)
data = data.dropna()


###############################################################################
#  Iterate

all_features = ['prma', 'disc', 'vol', 'rets_1day', 'rets_5day'] + spreads.columns.tolist()[1:]

all_dates = data.Date.unique()
train_dates = all_dates[:-15]
test_dates = all_dates[-15:]

all_groups = data.Group.unique()

output = pd.DataFrame()




for g in all_groups:

    data2 = data[data.Group == g].copy()

    train_data = data2[data2.Date.isin(train_dates)]
    test_data = data2[data2.Date.isin(test_dates)]

    X_train = train_data[all_features]
    y_train = train_data['UnivResponse']

    X_test = test_data[all_features]
    y_test = test_data['UnivResponse']

    tree = ExtraTreesClassifier()

    parameters = {
        'n_estimators': [10, 100],
        'min_samples_leaf': [30, 100, 1000],
        'criterion': ['gini', 'entropy'],
        'max_features': ['auto' ,'sqrt', 'log2'],
        'max_depth': [4, 10, 20, 40, 80]
    }

    print('Starting grid search for {}...'.format(g))
    clf = GridSearchCV(tree, parameters, n_jobs=3, verbose=1)
    clf.fit(X=X_train, y=y_train)

    probs = clf.predict_proba(X_test)[:, 1]

    output = output.append(pd.DataFrame({
        'Group': g,
        'Date': test_dates,
        'probs': probs,
        'Responses': y_test.values}))

    print(g)


# Attach forward returns for n_days
forward_rets = returns.rolling(window=3).sum().shift(-3)
forward_rets = forward_rets.unstack().reset_index()
forward_rets.columns = ['Group', 'Date', 'forward_ret']

output2 = output.merge(forward_rets)


# Select only top and bottom groups
# output2 = output2[output2.Group.apply(lambda x: x.split('_')[1] in ['1', '5'])]

# Get buy signals
select = output2.pivot(index='Date', columns='Group', values='probs')

select2 = (select.rank(axis=1, pct=True) > 0.90).astype(int)
select2 = select2.unstack().reset_index()
select2.columns = ['Group', 'Date', 'SelectTop']

output3 = output2.merge(select2)

select2 = (select.rank(axis=1, pct=True) < 0.10).astype(int)
select2 = select2.unstack().reset_index()
select2.columns = ['Group', 'Date', 'SelectBottom']

output3 = output3.merge(select2)

print(output3[output3.SelectTop == 1].forward_ret.mean())
print(output3[output3.SelectBottom == 1].forward_ret.mean())

