import itertools
import numpy as np
import pandas as pd
import datetime as dt


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


def ern_date_blackout(data, offset1=-1, offset2=2):
    assert offset1 <= 0, 'Offset1 must be less than/equal to 0'
    assert offset2 >= 0, 'Offset2 must be greater than/equal to 0'
    ern_inds = np.where(data.EARNINGSFLAG == 1)[0]
    all_inds = ern_inds.copy()
    for i in range(abs(offset1)):
        all_inds = np.append(all_inds, ern_inds-(i+1))
    for i in range(offset2):
        all_inds = np.append(all_inds, ern_inds+(i+1))
    all_inds = all_inds[all_inds >= 0]
    all_inds = all_inds[all_inds < data.shape[0]]
    blackouts = np.zeros(data.shape[0])
    blackouts[all_inds] = 1
    data['blackout'] = blackouts
    return data


def ern_price_anchor(data, init_offset=1, window=20):
    """
    Parameters
    ----------
    init_offset : int
        The index relative to the earnings date that represents the first
        anchor price
    window : int
        The maximum number of days to look back to create the anchor price
    """
    assert 'blackout' in data.columns
    closes = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    earningsflag = data.pivot(index='Date', columns='SecCode',
                              values='EARNINGSFLAG').fillna(0)
    blackout = data.pivot(index='Date', columns='SecCode',
                          values='blackout').fillna(0)
    # Get window period anchor price
    init_anchor = earningsflag.shift(init_offset).fillna(0) * closes
    end_anchor = earningsflag.shift(init_offset+window).fillna(0) * -1 * \
        closes.shift(window).fillna(0)
    init_anchor2 = (init_anchor + end_anchor).cumsum()
    output = closes.copy()
    output[:] = np.where(init_anchor2 == 0,
                         closes.shift(window-1), init_anchor2)
    output[:] = np.where(blackout, np.nan, output)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'anchor_price']
    data = data.merge(output)
    data['anchor_ret'] = data.AdjClose / data.anchor_price
    return data


def ern_return(data):
    """
    (T-1) to (T+1) Vwap return
    """
    prices = data.pivot(index='Date', columns='SecCode',
                        values='AdjVwap').fillna(method='pad')
    earningsflag = data.pivot(index='Date', columns='SecCode',
                              values='EARNINGSFLAG').fillna(0)
    rets = prices.shift(-1) / prices.shift(1)
    rets[:] = np.where(earningsflag == 1, rets, np.nan)
    rets = rets.fillna(method='pad').shift(2).fillna(1)
    output = rets.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'earnings_ret']
    return data.merge(output)


def make_anchor_ret_rank(data, init_offset=1,
                         window=20):
    data = ern_price_anchor(data, init_offset=init_offset,
                            window=window)
    data['anchor_ret'] = data.AdjClose / data.anchor_price - 1
    data['anchor_ret'] = data.anchor_ret.fillna(0)
    anchor_rets = data.pivot(index='Date',
                             columns='SecCode', values='anchor_ret')
    ranks = anchor_rets.rank(axis=1, pct=True)
    ranks = ranks.unstack().reset_index()
    ranks.columns = ['SecCode', 'Date', 'anchor_ret_rank']
    return data.merge(ranks)


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


def simple_responses(data, days=2):
    """
    Just return 1 or 0 for Position or Negative return
    """
    test_date_map = data[['Date', 'TestFlag']].drop_duplicates()
    assert isinstance(days, int)
    rets = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    rets2 = (rets.pct_change(days).shift(-days) >= 0).astype(int)
    output = rets2.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response']
    output = output.merge(test_date_map)
    return output


def make_variable_dict(data, variable, fillna=np.nan):
    data_pivot = data.pivot(index='Date', columns='SecCode', values=variable)
    if fillna == 'pad':
        data_pivot = data_pivot.fillna(method='pad')
    else:
        data_pivot = data_pivot.fillna(fillna)
    return data_pivot.T.to_dict()
