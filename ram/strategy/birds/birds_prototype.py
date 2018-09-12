import pandas as pd
import matplotlib.pyplot as plt

from ram.strategy.birds.main import BirdsStrategy
from ram.strategy.birds.data import get_features
from ram.strategy.birds.pairs import Pairs

from sklearn.ensemble import ExtraTreesClassifier




strategy = BirdsStrategy(strategy_code_version='version_0001',
                         prepped_data_version='version_0027')

strategy._get_prepped_data_file_names()

market_data = strategy.read_market_index_data()


features = pd.DataFrame()
# for i in range(len(strategy._prepped_data_files)):
for i in range(150, 180):
    data = strategy.read_data_from_index(i)
    f = get_features(data)
    f['tindex'] = i
    features = features.append(f)
    print(i)







features2 = features.pivot(index='Date',
                           columns='Group',
                           values='Feature')

prma = PRMA().fit(features2, 10)



plt.figure()
plt.plot(prma[['VOL40_1', 'VOL40_2', 'VOL40_3', 'VOL40_4', 'VOL40_5']])
plt.show()













if True:
    import pdb; pdb.set_trace()
    f = get_features(data)




train = features2.iloc[:-20]
test = features2.iloc[-20:]


# Create date book ends for months
# Get first days of month

all_dates = features2.index
month = [x.month for x in all_dates]
prev_month = [0] + month[:-1]
inds = np.array(month) != np.array(prev_month)
thresh_dates = all_dates[inds]

train_months = 12

test_returns = pd.DataFrame()
zscores = pd.DataFrame()

for i in range(train_months, len(thresh_dates)-1):

    d1 = thresh_dates[i-train_months]
    d2 = thresh_dates[i]
    d3 = thresh_dates[i+1]

    tr, z, pairs = Pairs().get_best_pairs(
        features2.loc[d1:d3],
        d2,
        z_window=50,
        max_pairs=1000)

    test_returns = test_returns.append(tr.iloc[:-1])
    zscores = zscores.append(z.iloc[:-1])

    print(i)
    break


plt.figure()
plt.plot(features2[['ARM_4', 'MFI15_3']].cumsum())
plt.show()

plt.figure()
plt.plot(zscores[['ARM_4_MFI15_3']])
plt.show()







cols = [3880, 3960, 4033, 4614, 4900, 7836]

for c in cols:
    col = zscores.columns[c]
    xx = zscores[col]
    xy = test_returns[col]

    xz = xx.copy()
    xz[:] = 0

    enter_thresh = 2
    exit_thresh = 1
    mult = 0

    for i in range(len(xz)):

        if np.isnan(xx.iloc[i]):
            mult = 0
            continue

        xz.iloc[i] = xy.iloc[i] * mult

        if (xx.iloc[i] > enter_thresh) & (mult == 0):
            mult = 1
        elif (xx.iloc[i] < exit_thresh) & (mult == 1):
            mult = 0
        elif (xx.iloc[i] < -exit_thresh) & (mult == 0):
            mult = -1
        elif (xx.iloc[i] > -exit_thresh) & (mult == -1):
            mult = 0

    print(xz.sum())


plt.figure()
plt.plot(xx)
plt.figure()
plt.plot(xy.cumsum())
plt.figure()
plt.plot(xz.cumsum())
plt.show()




# Stack

# Stack these


df1 = pd.DataFrame({'V1': range(4), 'V2': range(1, 5)})
df2 = pd.DataFrame({'V1': range(4), 'V3': range(1, 5)})



# PAIRS







test_returns2 = test_returns.copy()

entry_thresh = 2
exit_thresh = 1.5

for i in range(zscores.shape[1]):
    mult = 0
    for j in range(zscores.shape[0]):
        test_returns2.iloc[j, i] = test_returns2.iloc[j, i] * mult
        # for next period
        if mult == 0:
            if zscores.iloc[j, i] < -entry_thresh:
                mult = 1
            elif zscores.iloc[j, i] > entry_thresh:
                mult = -1

        elif mult == 1:
            if zscores.iloc[j, i] > -exit_thresh:
                mult = 0

        elif mult == -1:
            if zscores.iloc[j, i] < exit_thresh:
                mult = 0





inds = test_returns2.iloc[0]
inds[:] = 0
for i, d in enumerate(zscores.index):
    test_returns2.iloc[i] = test_returns2.iloc[i] * inds
    # Recalculate
    inds = np.where(zscores.iloc[i] < -thresh, 1, np.where(zscores.iloc[i] > thresh, -1, 0))





zscores['BOLL40_4_DISCOUNT100_4']
test_returns['BOLL40_4_DISCOUNT100_4']
test_returns2['BOLL40_4_DISCOUNT100_4']
















