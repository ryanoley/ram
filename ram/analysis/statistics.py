import os
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

from ram.utils.time_funcs import check_input_date

from gearbox.analysis_system import metrics


def get_stats(results):
    """
    `results` is a data frame of returns, not cum returns or PL
    """
    # Check input!
    assert isinstance(results, pd.DataFrame)
    assert results.isnull().sum().sum() == 0
    if isinstance(results.index[0], dt.date):
        results.index = pd.to_datetime(results.index)
    assert isinstance(results.index, pd.DatetimeIndex)

    out = pd.DataFrame(columns=[
        'Total', 'Mean', 'Std', 'Skew', 'Kurt',
        'Sharpe', 'Sortino', 'MinRet', 'WinP'])

    # TOTAL
    out.loc[:, 'Total'] = results.sum().round(4)

    # MOMENTS
    out.loc[:, 'Mean'] = results.mean().round(4)
    out.loc[:, 'Std'] = results.std().round(4)
    out.loc[:, 'Skew'] = results.skew().round(4)
    out.loc[:, 'Kurt'] = results.kurt().round(2)

    # FINANCE STATS
    out.loc[:, 'Sharpe'] = (results.mean() /
                            results.std() * np.sqrt(252)).round(4)
    out.loc[:, 'Sortino'] = (
        results.mean() /
        results.where(results < 0).fillna(0).std() * np.sqrt(252)).round(4)
    out.loc[:, 'MinRet'] = results.min().round(4)
    out.loc[:, 'WinP'] = (results > 0).mean().round(4)

    # DRAWDOWNS
    dd = _get_drawdowns(results)
    out = out.join(dd)

    # TIME AT HIGHS
    highs = _time_at_highs(results)
    out = out.join(highs)

    # PSEUDO VAR MEASURES
    var1 = _value_at_risk(results)
    out = out.join(var1)
    var2 = _c_value_at_risk(results)
    out = out.join(var2)
    var3 = _lower_partial_moment(results)
    out = out.join(var3)

    return out.T


def rollup_returns(data, column_num=0, agg_func='sum'):
    """
    Parameters
    ----------
    data : pd.DataFrame
        With Datetime-like object type in index
    column_num : int
        The iloc column
    agg_func : {'sum', 'mean'}
    """
    data = data.copy()
    column = data.columns[column_num]
    data['Year'] = [x.year for x in data.index]
    data['Qtr'] = [(x.month-1)/3+1 for x in data.index]
    data['QIndex'] = ['{}_{}'.format(i, j) for i, j
                      in zip(data.Year, data.Qtr)]
    if agg_func == 'sum':
        agg_data = data.groupby('QIndex')[column].sum().reset_index()
    elif agg_func == 'mean':
        agg_data = data.groupby('QIndex')[column].mean().reset_index()
    else:
        raise Exception('Aggregatation function not available')
    agg_data.columns = ['QIndex', 'values']
    agg_data['Year'] = [int(x.split('_')[0]) for x in agg_data.QIndex]
    agg_data['Qtr'] = [int(x.split('_')[1]) for x in agg_data.QIndex]
    return agg_data.pivot(index='Year', columns='Qtr', values='values')


###############################################################################
#  Stats implementations

def _get_drawdowns(df):

    df = df.cumsum().copy()

    outdf = pd.DataFrame(
        columns=['DD%', 'DDDays', 'UnderwaterDays', 'Underwater%'],
        index=df.columns)

    for col, vals in df.iteritems():
        dd, dd_days, dd_dates = metrics.draw_downs(vals, True)
        ind = np.argsort(dd)
        worst_dd = dd[ind][0]
        worst_dd_days = dd_days[ind][0]
        # Organize underwater periods
        ind = np.argsort(dd_days)[::-1]
        worst_underwater_dd = dd[ind][0]
        worst_underwater_days = dd_days[ind][0]
        outdf.loc[col] = (worst_dd, worst_dd_days,
                          worst_underwater_days, worst_underwater_dd)

    return outdf.astype(float)


def _time_at_highs(df):
    df = df.copy()
    df = df.cumsum()
    highs = np.maximum.accumulate(df)
    out = pd.DataFrame({'TimeAtHighs': (df == highs).sum() / len(df)})
    return out.round(3)


def _beta(returns, market):
    # Create a matrix of [returns, market]
    m = np.matrix([returns, market])
    # Return the covariance of m divided by the standard deviation
    # of the market returns
    return np.cov(m)[0][1] / np.std(market)


def _value_at_risk(returns):
    """
    What is the worst probable loss in a (1-alpha) situation
    """
    # Calculate the index associated with alpha
    alpha = 0.05
    index = int(alpha * len(returns))
    # Iterate through each column
    out = pd.DataFrame(index=returns.columns,
                       columns=['VaR_5perc', 'VaR_1perc'])
    for column in returns:
        out.loc[column, 'VaR_5perc'] = np.sort(returns[column])[index]
    # Get one percent
    alpha = 0.01
    index = int(alpha * len(returns))
    for column in returns:
        out.loc[column, 'VaR_1perc'] = np.sort(returns[column])[index]
    # VaR is positive in literature but I like looking at it as neg
    out = out.astype(float).round(4)
    return out


def _c_value_at_risk(returns):
    """
    Conditional VaR is the average of all the losses beyond the 0.05 VaR
    loss.
    """
    # Calculate the index associated with alpha
    alpha = 0.05
    index = max(int(alpha * len(returns)), 1)
    # Iterate through each column
    out = pd.DataFrame(index=returns.columns, columns=['CVaR_5perc'])
    for column in returns:
        out.loc[column, 'CVaR_5perc'] = np.mean(np.sort(
            returns[column])[:index])
    # VaR is positive in literature but I like looking at it as neg
    out = out.astype(float).round(4)
    return out


def _lower_partial_moment(returns):
    """
    Lower partial moment: Whereas measures of risk-adjusted return
    based on volatility treat all deviations from the mean as risk,
    measures of risk-adjusted return based on lower partial moments
    consider only deviations below some predefined minimum return
    threshold. For example, negative deviations from the mean is
    risky whereas positive deviations are not.

    A useful classification of measures of risk-adjusted returns
    based on lower partial moments in by their order. The larger
    the order the greater the weighting will be on returns that fall
    below the target threshold, meaning that larger orders result in
    more risk-averse measures.
    """
    threshold = 0
    order = 2
    # Get spreads between expectation and actual. Flip sign to represent
    # this risk number as positive being bad
    diff = -(returns-threshold)
    diff = diff * (diff > 0)
    diff = diff ** order
    out = pd.DataFrame({'LPM_2': diff.mean() * 1000}).astype(float).round(4)
    return out
