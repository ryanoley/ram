import numpy as np
import pandas as pd
import datetime as dt


def make_variable_dict(data, variable, fillna=np.nan):
    data_pivot = data.pivot(index='Date', columns='SecCode', values=variable)
    if fillna == 'pad':
        data_pivot = data_pivot.fillna(method='pad')
    else:
        data_pivot = data_pivot.fillna(fillna)
    return data_pivot.T.to_dict()


def outlier_rank(data, variable, outlier_std=4):
    """
    Will create two columns, and if the variable is an extreme outlier will
    code it as a 1 or -1 depending on side and force rank to median for
    the date.
    """
    pdata = data.pivot(index='Date', columns='SecCode', values=variable)

    # Clean
    daily_median = pdata.median(axis=1)

    # Allow to fill up to five days of missing data if there was a
    # previous
    pdata = pdata.fillna(method='pad', limit=5)

    # Fill missing values with median values if there is no data at all
    fill_df = pd.concat([daily_median] * pdata.shape[1], axis=1)
    fill_df.columns = pdata.columns
    pdata = pdata.fillna(fill_df)

    # For cases where everything is nan
    pdata = pdata.fillna(-999)

    # Get extreme value cutoffs
    daily_min = daily_median - outlier_std * pdata.std(axis=1)
    daily_max = daily_median + outlier_std * pdata.std(axis=1)

    # FillNans are to avoid warning
    extremes = pdata.fillna(-99999).gt(daily_max, axis=0).astype(int) - \
        pdata.fillna(99999).lt(daily_min, axis=0).astype(int)

    # Rank
    ranks = (pdata.rank(axis=1) - 1) / (pdata.shape[1] - 1)

    # Combine
    extremes = extremes.unstack().reset_index()
    extremes.columns = ['SecCode', 'Date', variable + '_extreme']
    ranks = ranks.unstack().reset_index()
    ranks.columns = ['SecCode', 'Date', variable]
    return ranks.merge(extremes)


def smoothed_responses(data, thresh=.25, days=[2, 4, 6]):
    test_date_map = data[['Date', 'TestFlag']].drop_duplicates()
    if not isinstance(days, list):
        days = [days]
    rets = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    for i in days:
        if i == days[0]:
            rank = rets.pct_change(i).shift(-i).rank(axis=1, pct=True)
        else:
            rank += rets.pct_change(i).shift(-i).rank(axis=1, pct=True)
    final_ranks = rank.rank(axis=1, pct=True)
    output = final_ranks.copy()
    output[:] = (final_ranks >= (1 - thresh)).astype(int) - \
        (final_ranks <= thresh).astype(int)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response']
    output = output.merge(test_date_map)
    return output
