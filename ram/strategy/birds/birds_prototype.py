import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools as it
import matplotlib.pyplot as plt

from ram.strategy.base import Strategy
from ram.data.feature_creator import *

from sklearn.ensemble import ExtraTreesClassifier


def make_groups(data, n_groups=5, n_days=3):
    nan_inds = data.isnull()
    # Create groups
    groups = data.rank(axis=1, pct=True) * n_groups
    groups[:] = np.ceil(groups)
    # Black-out days that aren't the re-balance days
    inds = np.arange(0, len(groups), n_days)
    # Remove final index if present because the next time index will
    # make groups on the first day
    if inds[-1] == (groups.shape[0] - 1):
        inds = inds[:-1]
    keep_dates = groups.index[inds]
    output = groups.copy()
    output[:] = np.nan
    output.loc[keep_dates] = groups.loc[keep_dates]
    output[nan_inds] = -999
    return output.fillna(method='pad')


def get_index_features(features, groups):
    features = features.unstack().reset_index()
    features.columns = ['SecCode', 'Date', 'Feature']
    # NO SHIFT. This means that at 345, stocks are sorted into groups,
    # and an index is created
    groups = groups.unstack().reset_index()
    groups.columns = ['SecCode', 'Date', 'Group']
    index_features = features.merge(groups)
    # Drop nan values (999s don't account for the shift that happens above)
    index_features = index_features[index_features.Feature.notnull()]
    index_features = index_features.groupby(['Group', 'Date'])['Feature'].mean().reset_index()
    index_features = index_features[index_features.Group != -999].reset_index(drop=True)
    return index_features


def get_index_returns(rets, groups):
    rets = rets.unstack().reset_index()
    rets.columns = ['SecCode', 'Date', 'DailyReturn']
    # Shift one day forward to affix the return
    groups = groups.shift(1).unstack().reset_index()
    groups.columns = ['SecCode', 'Date', 'Group']
    index_rets = rets.merge(groups)
    # Drop nan values (999s don't account for the shift that happens above)
    index_rets = index_rets[index_rets.DailyReturn.notnull()]
    index_rets = index_rets.groupby(['Group', 'Date'])['DailyReturn'].mean().reset_index()
    index_rets = index_rets[index_rets.Group != -999].reset_index(drop=True)
    return index_rets


def get_index_responses(features, n_days=3):
    returns = features.pivot(index='Date',
                             columns='Group',
                             values='DailyReturn')

    rolling_rets = returns.rolling(window=n_days).sum()
    ranks = rolling_rets.rank(axis=1, pct=True).shift(-n_days)
    nan_inds = ranks.isnull()
    bins = (ranks > 0.5).astype(int)
    bins[nan_inds] = np.nan
    bins = bins.unstack().reset_index()
    bins.columns = ['Group', 'Date', 'Response']
    return bins


def make_indexes(data, close_prices, test_dates, label):
    n_days = 3
    groups = make_groups(data, n_groups=5, n_days=n_days)
    features = get_index_features(data, groups)
    returns = get_index_returns(close_prices.pct_change(), groups)
    features = features.merge(returns, how='left')
    responses = get_index_responses(features, n_days=n_days)
    features = features.merge(responses, how='left')
    features['Group'] = features.Group.apply(lambda x: '{}_{}'.format(label, int(x)))
    features = features[features.Date.isin(test_dates)].reset_index(drop=True)
    # First day of daily return is not knowable, so delete for now to make
    # sure it is never relied upon
    features.DailyReturn.loc[features.Date == min(test_dates)] = np.nan
    return features


"""
TESTS

test_dates = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]]
label = 'VAR1'

data = pd.DataFrame(index=test_dates)
data['A'] = [1, 2, 3, 4, 5]
data['B'] = [6, 7, 8, 9, 10]
data['C'] = [11, 12, 13, 14, 16]
data['D'] = [16, 17, 18, 19, 20]
data['E'] = [21, 22, 23, 24, 25]

close_prices = data.copy()

if True:
    import pdb; pdb.set_trace()
    make_indexes(data, close_prices, test_dates, label)
"""

def extract_test_dates(data):
    test_dates = data.Date[data.TestFlag].unique()
    test_dates1 = [x for x in test_dates if x.month == test_dates[0].month]
    test_dates2 = [x for x in test_dates if x.month != test_dates[0].month]
    # Add first date of following month
    test_dates = test_dates1
    return test_dates


def get_features(data):

    test_dates = extract_test_dates(data)

    # Clean and rotate data
    open_ = clean_pivot_raw_data(data, 'AdjOpen')
    high = clean_pivot_raw_data(data, 'AdjHigh')
    low = clean_pivot_raw_data(data, 'AdjLow')
    close = clean_pivot_raw_data(data, 'AdjClose')
    volume = clean_pivot_raw_data(data, 'AdjVolume')
    avgdolvol = clean_pivot_raw_data(data, 'AvgDolVol')

    # RETURNS
    returns = close.pct_change(1)

    # FEATURE
    features = pd.DataFrame()

    for x in [5, 10, 20, 40, 80]:
        prma = PRMA().fit(close, x)
        prma = make_indexes(prma, close, test_dates, 'PRMA{}'.format(x))
        features = features.append(prma)


    for x in [10, 20, 40]:
        vol = VOL().fit(close, x)
        vol = make_indexes(vol, close, test_dates, 'VOL{}'.format(x))
        features = features.append(vol)


    # for x in [40, 100, 200]:
    #     disc = DISCOUNT().fit(close, x)
    #     disc = make_indexes(disc, close, test_date, 'DISCOUNT{}'.format(x))
    #     features = features.append(disc)


    for x in [10, 20, 40, 80]:
        boll = BOLL().fit(close, x)
        boll = make_indexes(boll, close, test_dates, 'BOLL{}'.format(x))
        features = features.append(boll)


    for x in [40, 80]:
        boll = BOLL_SMOOTH().fit(close, 2, x)
        boll = make_indexes(boll, close, test_dates, 'BOLL2{}'.format(x))
        features = features.append(boll)


    for x in [80, 160]:
        boll = BOLL_SMOOTH().fit(close, 4, x)
        boll = make_indexes(boll, close, test_dates, 'BOLL4{}'.format(x))
        features = features.append(boll)


    for x in [15, 30, 100]:
        rsi = RSI().fit(close, x)
        rsi = make_indexes(rsi, close, test_dates, 'RSI{}'.format(x))
        features = features.append(rsi)


    for x in [15, 30, 100]:
        mfi = MFI().fit(high, low, close, volume, x)
        mfi = make_indexes(mfi, close, test_dates, 'MFI{}'.format(x))
        features = features.append(mfi)


    # OTHER VARS variables
    variables = ['PE',
                 'ARM',
                 'ARMREVENUE',
                 'ARMRECS',
                 'ARMEARNINGS',
                 'ARMEXRECS',
                 'SIRANK',
                 'SISHORTSQUEEZE',
                 'SIINSTOWNERSHIP',
                 'MarketCap',
                 'AvgDolVol']

    for v in variables:
        var = clean_pivot_raw_data(data, v)
        var = make_indexes(var, close, test_dates, v)
        features = features.append(var)

    return features


###############################################################################

class BirdsStrategy(Strategy):

    def strategy_init(self):
        pass

    def get_data_blueprint_container(self):
        pass

    def get_strategy_source_versions(self):
        pass

    def process_raw_data(self, data, time_index, market_data=None):
        pass

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        pass

    def get_implementation_param_path(self):
        pass

    def process_implementation_params(self):
        pass


###############################################################################

class PairsStrategy1:

    def get_best_pairs(self,
                       data,
                       cut_date,
                       z_window,
                       max_pairs):
        train_close = data.loc[data.index < cut_date]
        pairs = self._get_stats_all_pairs(train_close)
        fpairs = self._filter_pairs(pairs, data, max_pairs)
        # Create daily z-scores
        test_rets, test_pairs = self._get_test_zscores(data, cut_date,
                                                       fpairs, z_window)
        return test_rets, test_pairs, fpairs

    def _filter_pairs(self, pairs, data, max_pairs):
        """
        Function is to score based on incoming stats
        """
        # Rank values
        rank1 = np.argsort(np.argsort(-pairs.corrcoef))
        rank2 = np.argsort(np.argsort(pairs.distances))
        pairs.loc[:, 'score'] = rank1 + rank2
        # Sort
        pairs = pairs.sort_values('score', ascending=True)
        pairs = pairs.iloc[:max_pairs].reset_index(drop=True)
        return pairs

    def _get_stats_all_pairs(self, data):
        # Convert to numpy array for calculations
        rets_a = np.array(data)
        index_a = np.array(data.cumsum())

        # Get matrix of all combos
        X1 = self._get_corr_coef(rets_a)
        X2 = np.apply_along_axis(self._get_corr_moves, 0, rets_a, rets_a)
        X3 = np.apply_along_axis(self._get_vol_ratios, 0, rets_a, rets_a)
        X4 = np.apply_along_axis(self._get_abs_distance, 0, index_a, index_a)

        # Output
        legs = zip(*it.combinations(data.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]

        # Capture going first down rows, then over columns
        # (Column, Row)
        z1 = list(it.combinations(range(len(data.columns)), 2))

        stat_df['corrcoef'] = [X1[z1[i]] for i in range(len(z1))]
        stat_df['corrmoves'] = [X2[z1[i]] for i in range(len(z1))]
        stat_df['volratio'] = [X3[z1[i]] for i in range(len(z1))]
        stat_df['distances'] = [X4[z1[i]] for i in range(len(z1))]

        return stat_df

    @staticmethod
    def _get_abs_distance(x_index, indexes):
        return np.sum(np.abs(x_index[:, None] - indexes), axis=0)

    @staticmethod
    def _get_corr_coef(rets):
        """
        Returns correlation coefficients of first column vs rest
        """
        return np.corrcoef(rets.T)

    @staticmethod
    def _get_corr_moves(x_ret, rets):
        """
        Returns percentage of moves that are the same of first column vs rest
        """
        # Percent ups and downs that match
        Z = (rets >= 0).astype(int) + (x_ret[:, None] >= 0).astype(int)
        return ((Z != 1).sum(axis=0) / float(len(Z)))

    @staticmethod
    def _get_vol_ratios(x_ret, rets):
        """
        Returns return series volatility, first column over rest columns
        """
        x_std = np.array([np.std(x_ret)] * rets.shape[1])
        all_std = np.std(rets, axis=0)
        # Flipped when it is pulled from nXn
        return all_std / x_std

    def _get_test_zscores(self, data, cut_date, fpairs, window):
        # Create two data frames that represent Leg1 and Leg2
        df_leg1 = (data.loc[:, fpairs.Leg1].cumsum() + 1) * 100
        df_leg2 = (data.loc[:, fpairs.Leg2].cumsum() + 1) * 100
        outdf = self._get_spread_zscores(df_leg1, df_leg2, window)
        # Get returns
        rets_leg1 = data.loc[:, fpairs.Leg1].copy()
        rets_leg2 = data.loc[:, fpairs.Leg2].copy()
        outdf_rets = rets_leg1 - rets_leg2.values
        # Add correct column names
        outdf.columns = ['{0}_{1}'.format(x, y) for x, y in
                         zip(fpairs.Leg1, fpairs.Leg2)]
        outdf_rets.columns = ['{0}_{1}'.format(x, y) for x, y in
                              zip(fpairs.Leg1, fpairs.Leg2)]
        return outdf_rets.loc[outdf_rets.index >= cut_date], \
            outdf.loc[outdf.index >= cut_date]

    def _get_spread_zscores(self, close1, close2, window):
        """
        Simple normalization
        """
        spreads = np.subtract(np.log(close1), np.log(close2))
        ma, std = self._get_moving_avg_std(spreads, window)
        return (spreads - ma) / std

    @staticmethod
    def _get_moving_avg_std(X, window):
        """
        Optimized calculation of rolling mean and standard deviation.
        """
        ma_df = X.rolling(window=window).mean()
        std_df = X.rolling(window=window).std()
        return ma_df, std_df




###############################################################################

strategy = BirdsStrategy(strategy_code_version='version_0001',
                         prepped_data_version='version_0027')

strategy._get_prepped_data_file_names()

market_data = strategy.read_market_index_data()


features = pd.DataFrame()
# for i in range(len(strategy._prepped_data_files)):
for i in range(150, 180):
    data = strategy.read_data_from_index(i)
    import pdb; pdb.set_trace()
    f = get_features(data)
    f['tindex'] = i
    features = features.append(f)
    print(i)





features2 = features.pivot(index='Date',
                           columns='Group',
                           values='DailyReturn')



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

    tr, z, pairs = PairsStrategy1().get_best_pairs(
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

















###############################################################################




import seaborn as sns
sns.set()


index_returns = features.pivot(index='Date',
                              columns='Group',
                              values='DailyReturn')


plt.figure()
plt.plot(index_returns.cumsum())
plt.show()




ax = sns.heatmap(index_returns.corr())
plt.show()


