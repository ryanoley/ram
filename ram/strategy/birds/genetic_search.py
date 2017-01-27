import numpy as np
import pandas as pd
import datetime as dt
from random import randint, random, randint

###############################################################################

def init_population(count, n_confs):
    """
    Create a number of individuals (i.e. a population).
    where the weights sum to zero (across rows)
    """
    weightsL = np.random.rand(count, n_confs)
    weightsL = weightsL / weightsL.sum(axis=1)[:, np.newaxis]

    weightsS = np.random.rand(count, n_confs)
    weightsS = weightsS / weightsS.sum(axis=1)[:, np.newaxis]

    return [(w1, w2) for w1, w2 in zip(weightsL, weightsS)]


@profile
def evolve_and_score(pop, ret_data, estsL, rowsL, estsS, rowsS, port_size):

    ##  Parameters
    retain = 0.2
    random_select = 0.05
    mutate = 0.01

    ##  Score
    scores = []
    ind = 0  # Temp fix for duplicate values
    for wL, wS in pop:
        estsL2 = make_weighted_estimate(estsL, wL)
        estsS2 = make_weighted_estimate(estsS, wS)

        sig_rowsL = get_min_est_rows(-1 * estsL2, rowsL, topX=port_size)
        sig_rowsS = get_min_est_rows(estsS2, rowsS, topX=port_size)

        sharpe = get_long_short_sharpe(ret_data.iloc[sig_rowsL],
                                       ret_data.iloc[sig_rowsS])
        scores.append((sharpe, ind, (wL, wS)))
        ind += 1

    ##  Retain top values
    report_score = sum([x[0] for x in scores]) / float(len(scores))
    scores = [x[2] for x in sorted(scores)[::-1]]

    retain_length = int(len(scores)*retain)
    parents = scores[:retain_length]

    # randomly add other individuals to
    # promote genetic diversity
    for individual in scores[retain_length:]:
        if random_select > random():
            parents.append(individual)

    # mutate some individuals
    for individual in parents:
        if mutate > random():
            pos_to_mutateL = randint(0, len(individual[0])-1)
            pos_to_mutateS = randint(0, len(individual[0])-1)
            individual[0][pos_to_mutateL] = random()
            individual[1][pos_to_mutateL] = random()

    # crossover parents to create children
    parents_length = len(parents)
    desired_length = len(pop) - parents_length
    children = []

    while len(children) < desired_length:
        male = randint(0, parents_length-1)
        female = randint(0, parents_length-1)
        if male != female:
            male = parents[male]
            female = parents[female]
            half = len(male[0]) / 2
            child = (
                np.append(male[0][:half], female[0][half:]),
                np.append(male[1][:half], female[1][half:])
            )
            children.append(child)
    parents.extend(children)
    # Ensure all sum to one
    output = []
    for weights in parents:
        output.append((
            weights[0] / weights[0].sum(),
            weights[1] / weights[1].sum()
        ))
    return output, report_score




###############################################################################

@profile
def get_long_short_sharpe(longdf, shortdf):
    long_ = get_daily_pl(longdf)
    short = get_daily_pl(shortdf)
    rets = long_[1] - short[1]
    return np.mean(rets) / np.std(rets)


@profile
def get_daily_pl(data):
    dates = np.concatenate((data['T_1'].values,
                            data['T_2'].values,
                            data['T_3'].values))
    rets = np.concatenate((data['ReturnDay1'].values,
                           data['ReturnDay2'].values,
                           data['ReturnDay3'].values))
    dates, _, rets = _bucket_mean(dates, rets)
    return dates, rets

@profile
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

def get_min_est_rows(ests, rows, topX=10):
    inds = np.argsort(ests, axis=1)
    return np.unique(np.concatenate([rows[i][inds[i][:topX]] for i in range(len(rows))]))

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


def get_optimal_combination(df, estsL, estsS, port_size=2, epochs=30):
    ##  Parameters
    pop_size = 100

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

    ##  Init population
    p = init_population(pop_size, estsL.shape[1])

    ##  Iterate x Epochs
    fitness_history = []
    for i in xrange(epochs):
        p, score = evolve_and_score(p, df, estsL2, rowsL,
                                    estsS2, rowsS, port_size)
        fitness_history.append(score)
        print score

    return p[0]
