import numpy as np
import pandas as pd


def get_daily_returns(data, feature_ndays, holding_ndays, n_per_side):

    close_data = data.pivot(index='Date',
                            columns='SecCode',
                            values='AdjClose')

    open_data = data.pivot(index='Date',
                           columns='SecCode',
                           values='AdjOpen')

    features = open_data / close_data.shift(feature_ndays)

    vwap_data = data.pivot(index='Date',
                           columns='SecCode',
                           values='AdjVwap')

    # Returns values
    entry_returns = close_data / vwap_data - 1
    normal_returns = close_data.pct_change()
    exit_returns = vwap_data / close_data.shift(1) - 1

    # Same quarter test dates
    unique_test_dates = data.Date[data.TestFlag].drop_duplicates()
    qtrs = np.array([(x.month-1)/3+1 for x in unique_test_dates])
    unique_test_dates2 = unique_test_dates.values[qtrs == qtrs[0]]

    for i, d in enumerate(unique_test_dates2):
        daily_feature = features.loc[d].copy()
        daily_feature.sort_values(inplace=True)
        long_ids = daily_feature.iloc[:n_per_side].index.tolist()
        short_ids = daily_feature.iloc[-n_per_side:].index.tolist()

        period_dates = []
        period_rets = []
        for j in range(i, i+holding_ndays+1):
            period_dates.append(unique_test_dates[j])
            if j == i:
                pass
            elif j == (i+holding_ndays):
                pass
            else:
                pass
