import os
import numpy as np
import pandas as pd
import datetime as dt
from gearbox import convert_date_array, create_time_index



def ern_date_blackout(data, offset1=-1, offset2=1):
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


def make_anchor_ret_rank(data, init_offset=1, window=20):
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
    output[:] = np.where(init_anchor2 == 0, closes.shift(window-1), init_anchor2)
    output[:] = np.where(blackout, np.nan, output)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'anchor_price']
    data = data.merge(output)
    data['anchor_ret'] = data.AdjClose / data.anchor_price
    return data


# Revisions to Smart Estimates
def get_se_revisions(data, se_col, out_col, window=10):
    '''
    Cumulative sum of estimate changes over window following
    the earnings announcement
    '''
    ernflag = data.pivot(index='Date', columns='SecCode',
                         values='EARNINGSFLAG')
    fq1_estimates = data.pivot(index='Date', columns='SecCode',
                               values='{}FQ1'.format(se_col))
    fq2_estimates = data.pivot(index='Date', columns='SecCode',
                               values='{}FQ2'.format(se_col))
    fq1_estimates[:] = np.where(ernflag.shift(-1) == 1, fq2_estimates,
                                fq1_estimates)
    delta = fq1_estimates.diff(1).fillna(0.)

    # Sum estimate changes in  window following ern
    delta_sum_window = ernflag.rolling(window = window, min_periods=1).sum()
    delta[:] = np.where(delta_sum_window == 1, delta, 0.)
    delta_sum = delta.rolling(window=window, min_periods=1).sum()
    delta_sum[:] = np.where(delta_sum_window == 1, delta_sum, 0.)
    delta_sum = delta_sum.unstack().reset_index()
    delta_sum.columns = ['SecCode', 'Date', out_col]
    data = data.merge(delta_sum, how='left')
    return data


def get_previous_ern_return(data, fillna=False, prior_data=[]):
    '''
    Get prior earnings return, fillna with 0. if specified
    '''
    req_cols = ['SecCode', 'Date', 'EARNINGSFLAG', 'EARNINGSRETURN']
    assert set(req_cols).issubset(data.columns)
    train = data[req_cols].copy()

    if len(prior_data) > 0:
        assert set(req_cols).issubset(prior_data.columns)
        prior_data = prior_data[req_cols]
        train = prior_data.append(train).drop_duplicates(['Date','SecCode'])

    ernflag = train.pivot(index='Date', columns='SecCode', values='EARNINGSFLAG')
    ernret = train.pivot(index='Date', columns='SecCode', values='EARNINGSRETURN')
    ernret[:] = np.where(ernflag==1, ernret, np.nan)
    ernret = ernret.shift(1).fillna(method='pad')
    prevRets = ernret.unstack().reset_index()
    prevRets.columns = ['SecCode', 'Date', 'PrevRet']
    if fillna:
        prevRets.fillna(0., inplace=True)
    data = data.merge(prevRets, how='left')
    return data


def get_vwap_returns(data, days, hedged=False, market_data=None):
    exit_col = 'LEAD{}_AdjVwap'.format(days)
    ret_col = 'Ret{}'.format(days)
    
    if exit_col not in data.columns:
        print 'Lead columns not available for {} days'.format(days)
        return data
    prices  = data[['SecCode', 'Date', exit_col, 'LEAD1_AdjVwap']].copy()
    prices[ret_col] = (prices[exit_col] / prices.LEAD1_AdjVwap)

    if hedged:
        if exit_col not in market_data.columns:
            print 'SPY Lead columns not available for {} days'.format(days)
            return data
        spy_prices  = market_data[['Date', exit_col, 'LEAD1_AdjVwap']].copy()
        spy_prices['MktRet'] = (spy_prices[exit_col] / spy_prices.LEAD1_AdjVwap)
        prices = prices.merge(spy_prices[['Date', 'MktRet']], how='left')
        prices[ret_col] -= prices.MktRet
    
    data = data.merge(prices[['SecCode', 'Date', ret_col]], how='left')
    return data



############# RESPONSES ##################

def smoothed_responses(data, thresh=.25, days=[2, 3]):
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
    return output


def fixed_response(data, days=5):
    assert 'Ret{}'.format(days) in data.columns
    data['Response'] = data['Ret{}'.format(days)].copy()
    return data[['SecCode', 'Date', 'Response']].copy()

