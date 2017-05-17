import os
import itertools
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

from ram.strategy.statarb.main import StatArbStrategy
from ram.strategy.statarb.pairselector.pairs3 import PairSelector3

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression


strategy = StatArbStrategy('version_0017', False)
strategy._get_data_file_names()
pairselector = PairSelector3()



# ~~~~~~ Import some data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

index = 3

data = strategy.read_data_from_index(index)


train_dates = data.Date[~data.TestFlag].drop_duplicates().values
test_dates = data.Date[data.TestFlag].drop_duplicates().values

# Test dates for only one quarter forward
qtrs = np.array([(x.month-1)/3 + 1 for x in test_dates])
test_dates = test_dates[qtrs == qtrs[0]]

pairselector.rank_pairs(data, True)


close1 = pairselector.close_data[pairselector.pair_info.Leg1]
close2 = pairselector.close_data[pairselector.pair_info.Leg2]

spreads = close1.copy()
spreads[:] = (close1 / close1.iloc[0]).values - (close2 / close2.iloc[0]).values


plt.figure()
plt.plot(spreads.iloc[:, :50])
plt.show()




# ~~~~~~ Responses ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from ram.strategy.statarb.responses.response1 import response_strategy_1
from ram.strategy.statarb.responses.response2 import response_strategy_2

# Get train close prices for response creation
close1 = pairselector.close_data[pairselector.pair_info.Leg1]
close2 = pairselector.close_data[pairselector.pair_info.Leg2]
zscores = pairselector.get_zscores(40)



#resp1, resp2 = response_strategy_1(close1, close2, 0.05, 6)
if True:
    import pdb; pdb.set_trace()
    resp1, resp2 = response_strategy_2(close1, close2, 20, 2, 1)


# ~~~~~~ How do responses look when major move? ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


close1 = pairselector.close_data[pairselector.pair_info.Leg1]
close2 = pairselector.close_data[pairselector.pair_info.Leg2]

rets = close1.copy()
rets[:] = close1.pct_change().fillna(0).values - close2.pct_change().fillna(0).values
rets = rets.loc[test_dates]

inds = rets.values < -0.05

out = pd.DataFrame(columns=['Date', 'Mean1', 'Mean2', 'Count1', 'Count2'])

for i in range(inds.shape[0]):
    rets1 = resp1.loc[test_dates].iloc[i][inds[i]].dropna()
    rets2 = resp2.loc[test_dates].iloc[i][inds[i]].dropna()
    out.loc[i, 'Date'] = resp1.index[i]
    out.loc[i, 'Mean1'] = rets1.mean()
    out.loc[i, 'Mean2'] = rets2.mean()
    out.loc[i, 'Count1'] = len(rets1)
    out.loc[i, 'Count2'] = len(rets2)










# ~~~~~~ Plotting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

close1 = pairselector.close_data[pairselector.pair_info.Leg1]
close2 = pairselector.close_data[pairselector.pair_info.Leg2]

close1a = close1 / close1.iloc[0]
close2a = close2 / close2.iloc[0]

index = 206

plt.figure()
plt.plot(close1a.iloc[:, index])
plt.plot(close2a.iloc[:, index])
plt.show()

