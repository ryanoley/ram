import os
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

from ram.utils.time_funcs import check_input_date

from gearbox.analysis_system import metrics


def create_strategy_report(results, name, path):

    assert isinstance(results, pd.DataFrame)
    if not isinstance(results.index[0], dt.datetime):
        results.index = [check_input_date(x) for x in results.index]
    results = results.astype(float)

    # STATISTICS
    _get_stats(results).to_csv(os.path.join(path, 'results_{0}.csv'.format(name)))
    _get_stats(results.loc[dt.datetime(2012, 1, 1):]).to_csv(
        os.path.join(path, 'results_{0}_since2012.csv'.format(name)))

    # PLOTTING
    _make_plot(results, name, path)


def _get_stats(results):
    out = pd.DataFrame(columns=[
        'Total', 'Mean', 'Std', 'Skew', 'Kurt',
        'Sharpe', 'Sortino', 'MinRet', 'WinP'])

    # TOTAL
    out.loc[:, 'Total'] = results.sum()

    # MOMENTS
    out.loc[:, 'Mean'] = results.mean()
    out.loc[:, 'Std'] = results.std()
    out.loc[:, 'Skew'] = results.skew()
    out.loc[:, 'Kurt'] = results.kurt()

    # FINANCE STATS
    out.loc[:, 'Sharpe'] = results.mean() / results.std() * np.sqrt(252)
    out.loc[:, 'Sortino'] = results.mean() / \
        results.where(results < 0).fillna(0).std() * np.sqrt(252)
    out.loc[:, 'MinRet'] = results.min()
    out.loc[:, 'WinP'] = (results > 0).mean()

    # DRAWDOWNS
    dd = _get_drawdowns(results)
    out = out.join(dd)

    return out.T


def _get_drawdowns(df):

    df = df.copy()
    df = np.log(df.cumsum() + 1)

    outdf = pd.DataFrame(
        columns=['DD%', 'DDDays', 'UnderwaterDays', 'Underwater%'],
        index=df.columns)

    for col, vals in df.iteritems():
        dd, dd_days = metrics.draw_downs(vals.values)
        ind = np.argsort(dd)
        worst_dd = dd[ind][0]
        worst_dd_days = dd_days[ind][0]
        # Organize underwater periods
        ind = np.argsort(dd_days)[::-1]
        worst_underwater_dd = dd[ind][0]
        worst_underwater_days = dd_days[ind][0]
        outdf.loc[col] = (worst_dd, worst_dd_days,
                          worst_underwater_days, worst_underwater_dd)

    return outdf


def _make_plot(data, name, spath):
    data = data.copy()
    data = data.cumsum()

    plt.figure()
    for col, vals in data.iteritems():
        plt.plot(vals, label=col)
    plt.grid()
    if data.shape[1] < 6:
        plt.legend(loc=2)
    plt.savefig('{0}/{1}'.format(spath, 'results_plot_{0}'.format(name)))



def get_time_at_highs(cumpl):
    highs = np.maximum.accumulate(cumpl)
    return pd.Series((cumpl == highs).sum() / float(len(cumpl)),
                     name='TimeAtHighs')


def Xbeta(returns, market):
    # Create a matrix of [returns, market]
    m = np.matrix([returns, market])
    # Return the covariance of m divided by the standard deviation
    # of the market returns
    return np.cov(m)[0][1] / np.std(market)


def value_at_risk(returns, alpha=0.05):
    """
    What is the worst probable loss in a (1-alpha) situation
    """
    # Calculate the index associated with alpha
    index = int(alpha * len(returns))
    # Iterate through each column
    out = pd.Series(index=returns.columns, name='VaR_5perc')
    for column in returns:
        out[column] = np.sort(returns[column])[index]
    # VaR is positive in literature but I like looking at it as neg
    return out


def c_value_at_risk(returns, alpha=0.05):
    """
    Conditional VaR is the average of all the losses beyond the 0.05 VaR
    loss.
    """
    # Calculate the index associated with alpha
    index = int(alpha * len(returns))
    # Iterate through each column
    out = pd.Series(index=returns.columns, name='CVaR_5perc')
    for column in returns:
        out[column] = np.mean(np.sort(returns[column])[:index])
    # VaR is positive in literature but I like looking at it as neg
    return out


def lower_partial_moment(returns, threshold=0, order=1):
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
    # Get spreads between expectation and actual. Flip sign to represent
    # this risk number as positive being bad
    diff = -(returns-threshold)
    diff = diff * (diff > 0)
    diff = diff ** order
    out = diff.mean()
    out.name = 'LPM_1'
    return out

