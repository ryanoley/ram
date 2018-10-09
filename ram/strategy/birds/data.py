import numpy as np
import pandas as pd
import datetime as dt

from ram.data.feature_creator import *


def make_groups(data, n_groups=5, n_days=3):
    """
    Takes in DataFrame with dates in index and SecCodes in columns.
    Groups each SecCode by the feature, but only ever n_days days. The
    first date (index[0]) is always the first date of group creation,
    and this should be the first date of the TEST data.
    """
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
    """
    Takes in the same dataset as `make_groups`, but then also uses the
    groupings to create an average value for each of those groups.
    """
    features = features.unstack().reset_index()
    features.columns = ['SecCode', 'Date', 'Feature']
    groups = groups.unstack().reset_index()
    groups.columns = ['SecCode', 'Date', 'Group']
    index_features = features.merge(groups)
    # Drop nan values (999s don't account for the shift that happens above)
    index_features = index_features[index_features.Feature.notnull()]
    index_features = index_features[index_features.Group != -999]
    index_features = index_features.groupby(
        ['Group', 'Date'])['Feature'].mean().reset_index()
    return index_features


def get_index_returns(rets, groups):
    """
    Takes in a returns DataFrame with dates in the index and SecCodes in
    the columns. The groups DataFrame is the output from `make_groups`.
    """
    rets = rets.unstack().reset_index()
    rets.columns = ['SecCode', 'Date', 'DailyReturn']
    # Shift one day forward to affix the return
    groups = groups.shift(1).unstack().reset_index()
    groups.columns = ['SecCode', 'Date', 'Group']
    index_rets = rets.merge(groups)
    # Drop nan values (999s don't account for the shift that happens above)
    index_rets = index_rets[index_rets.DailyReturn.notnull()]
    index_rets = index_rets[index_rets.Group != -999]
    index_rets = index_rets.groupby(
        ['Group', 'Date'])['DailyReturn'].mean().reset_index()
    return index_rets


def get_index_responses(features, n_days=3, label='GroupResponse'):
    """
    Takes in the results of the get_index_features and get_index_returns
    to create responses for these indexes
    """
    returns = features.pivot(index='Date',
                             columns='Group',
                             values='DailyReturn')
    rolling_rets = returns.rolling(window=n_days).sum().shift(-n_days)
    ranks = rolling_rets.rank(axis=1, pct=True)
    nan_inds = ranks.isnull()
    bins = (ranks > 0.5).astype(int)
    bins[nan_inds] = np.nan
    bins = bins.unstack().reset_index()
    bins.columns = ['Group', 'Date', label]
    return bins


def make_indexes(data, close_prices, test_dates, label, n_groups, n_days):
    groups = make_groups(data, n_groups=n_groups, n_days=n_days)
    features = get_index_features(data, groups)
    returns = get_index_returns(close_prices.pct_change(), groups)
    features = features.merge(returns, how='left')
    responses = get_index_responses(features, n_days=n_days)
    features = features.merge(responses, how='left')
    features['Group'] = features.Group.apply(
        lambda x: '{}_{}'.format(label, int(x)))
    features = features[features.Date.isin(test_dates)].reset_index(drop=True)
    # First day of daily return is not knowable, so delete for now to make
    # sure it is never relied upon
    features.loc[features.Date == min(test_dates), 'DailyReturn'] = np.nan
    features.loc[features.Date == max(test_dates), 'Feature'] = np.nan
    features.loc[features.Date == max(test_dates), 'GroupResponse'] = np.nan
    return features


def extract_test_dates(data):
    test_dates = data.Date[data.TestFlag].unique()
    test_dates1 = [x for x in test_dates if x.month == test_dates[0].month]
    test_dates2 = [x for x in test_dates if x.month != test_dates[0].month]
    # Add first date of following month
    test_dates = test_dates1 + test_dates2[:1]
    return test_dates


###############################################################################

def get_features(data, n_groups, n_days):

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
        prma = make_indexes(prma, close, test_dates, 'PRMA{}'.format(x), n_groups, n_days)
        features = features.append(prma)

    for x in [10, 20, 40]:
        vol = VOL().fit(close, x)
        vol = make_indexes(vol, close, test_dates, 'VOL{}'.format(x), n_groups, n_days)
        features = features.append(vol)

    # Not in use because groupings became non-uniform
    # for x in [40, 100, 200]:
    #     disc = DISCOUNT().fit(close, x)
    #     disc = make_indexes(disc, close, test_date, 'DISCOUNT{}'.format(x), n_groups, n_days)
    #     features = features.append(disc)

    for x in [10, 20, 40, 80]:
        boll = BOLL().fit(close, x)
        boll = make_indexes(boll, close, test_dates, 'BOLL{}'.format(x), n_groups, n_days)
        features = features.append(boll)

    for x in [40, 80]:
        boll = BOLL_SMOOTH().fit(close, 2, x)
        boll = make_indexes(boll, close, test_dates, 'BOLL2{}'.format(x), n_groups, n_days)
        features = features.append(boll)

    for x in [80, 160]:
        boll = BOLL_SMOOTH().fit(close, 4, x)
        boll = make_indexes(boll, close, test_dates, 'BOLL4{}'.format(x), n_groups, n_days)
        features = features.append(boll)

    for x in [15, 30, 100]:
        rsi = RSI().fit(close, x)
        rsi = make_indexes(rsi, close, test_dates, 'RSI{}'.format(x), n_groups, n_days)
        features = features.append(rsi)

    for x in [15, 30, 100]:
        mfi = MFI().fit(high, low, close, volume, x)
        mfi = make_indexes(mfi, close, test_dates, 'MFI{}'.format(x), n_groups, n_days)
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
        var = make_indexes(var, close, test_dates, v, n_groups, n_days)
        features = features.append(var)

    # Get responses across all groups
    all_groups = get_index_responses(features, label='UnivResponse')
    features = features.merge(all_groups)

    return features
