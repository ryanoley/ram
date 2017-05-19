import numpy as np
import pandas as pd


def get_daily_returns(data, feature_ndays, holding_ndays, n_per_side):

    close_data = data.pivot(index='Date',
                            columns='SecCode',
                            values='AdjClose')

    open_data = data.pivot(index='Date',
                           columns='SecCode',
                           values='AdjOpen')

    vwap_data = data.pivot(index='Date',
                           columns='SecCode',
                           values='AdjVwap')

    #features = open_data / close_data.shift(feature_ndays)
    features = close_data / close_data.shift(feature_ndays)
    # Returns values
    #entry_returns = close_data / vwap_data - 1
    normal_returns = close_data.pct_change()
    #exit_returns = vwap_data / close_data.shift(1) - 1

    # Same quarter test dates
    unique_test_dates = data.Date[data.TestFlag].drop_duplicates()
    qtrs = np.array([(x.month-1)/3+1 for x in unique_test_dates])
    unique_test_dates2 = unique_test_dates.values[qtrs == qtrs[0]]
    unique_test_dates = unique_test_dates.values

    total_rets = pd.DataFrame(0, columns=['Ret'],
                              index=unique_test_dates)

    for i in range(len(unique_test_dates2)-holding_ndays):

        start_date = unique_test_dates[i]
        end_date = unique_test_dates[i+holding_ndays]

        daily_feature = features.loc[start_date].copy()
        if np.all(daily_feature.isnull()):
            continue

        daily_feature.sort_values(inplace=True)
        # Filter
        daily_feature = daily_feature.dropna()
        daily_feature = daily_feature[np.abs(daily_feature-1) <= .3]

        long_ids = daily_feature.iloc[:n_per_side].index.tolist()
        short_ids = daily_feature.iloc[-n_per_side:].index.tolist()

        day_rets = normal_returns.loc[start_date:end_date].copy()
        day_rets.iloc[0] = 0

        rets = (day_rets.loc[:, long_ids].mean(axis=1) -
                day_rets.loc[:, short_ids].mean(axis=1)) / holding_ndays

        rets[start_date] -= 0.0002 / holding_ndays
        rets[end_date] -= 0.0002 / holding_ndays

        total_rets.loc[rets.index, 'Ret'] += rets

    return total_rets
