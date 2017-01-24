import numpy as np
import pandas as pd
import datetime as dt

###############################################################################

def init_population(count, n):
    """
    Create a number of individuals (i.e. a population).
    where the weights sum to zero (across rows)
    """
    weights = np.random.rand(count, n)
    return weights / weights.sum(axis=1)[:, np.newaxis]



###############################################################################


def get_long_short_sharpe(longdf, shortdf):
    long_ = get_daily_pl(longdf)
    short = get_daily_pl(shortdf)
    rets = long_[1] - short[1]
    return np.mean(rets) / np.std(rets)


def get_daily_pl(data):
    dates = np.concatenate((data['T_1'].values,
                            data['T_2'].values,
                            data['T_3'].values))
    rets = np.concatenate((data['ReturnDay1'].values,
                           data['ReturnDay2'].values,
                           data['ReturnDay3'].values))
    dates, _, rets = _bucket_mean(dates, rets)
    return dates, rets



def _bucket_mean(x, y):
    """
    Buckets over x array, counts, and takes the mean of y array, and returns
    counts.
    """
    # Sort both arrays
    inds = np.argsort(x)
    x = np.take(x, inds)
    y = np.take(y, inds)
    # Get uniq and ordered values, and the indexes to reduce at
    uniq_x, reduce_at, counts = np.unique(
        x, return_index=True, return_counts=True)
    # Calculate mean
    mean_y = np.add.reduceat(y, reduce_at, dtype=np.float_) / counts
    return uniq_x, counts, mean_y


###############################################################################

def get_signals(ests, rows, topX=10):
    inds = np.argsort(ests, axis=1)
    n = inds.shape[0]
    return np.take(rows, inds, axis=1)[range(n), range(n), :topX]


###############################################################################

def make_estimate_arrays(df, estsL):

    estsL = estsL.copy()

    long_cols = estsL.columns

    estsL['SecCode'] = df.SecCode
    estsL['Date'] = df.Date
    estsL['RowNumber'] = range(len(estsL))

    for col in long_cols:
        if col == long_cols[0]:
            longs = np.array([estsL.pivot(
                index='Date', columns='SecCode', values=col).values])
        else:
            longs = np.vstack((longs, np.array([estsL.pivot(
                index='Date', columns='SecCode', values=col).values])))

    longs_row_nums = estsL.pivot(index='Date', columns='SecCode', values='RowNumber').values

    return longs, longs_row_nums


def make_weighted_estimate(ests, weights):
    return (ests * weights[:, None, None]).mean(axis=0)



def get_optimal_combination(df, estsL, estsS):

    ##  Assert columns have proper rows in them
    assert np.all(pd.Series([
        'SecCode', 'Date', 'T_1', 'T_2', 'T_3',
        'ReturnDay1', 'ReturnDay2', 'ReturnDay3']).isin(df.columns))

    assert estsL.shape[0] == estsS.shape[0]
    assert estsL.shape[1] == estsS.shape[1]
    assert estsL.shape[0] == df.shape[0]

    ##  Create stacked estimates
    estsL2, rowsL = make_estimate_arrays(df, estsL)
    estsS2, rowsS = make_estimate_arrays(df, estsS)

    # Get population of weights - First half
    n_confs = estsL.shape[1] * 2
    #weights = init_population(2, n_confs)
    weights = np.array([[2, 0, 1, 1], [1, 10, 1, 1]])

    w = weights[0]

    estsL3 = make_weighted_estimate(estsL2, w[:2])
    estsS3 = make_weighted_estimate(estsS2, w[2:])

    signalsL = get_signals(-1 * estsL3, rowsL, topX=1)
    signalsS = get_signals(estsS3, rowsS, topX=1)

    long_rets = df.iloc[signalsL.ravel()]
    short_rets = df.iloc[signalsS.ravel()]

    z = get_long_short_sharpe(long_rets, short_rets)

