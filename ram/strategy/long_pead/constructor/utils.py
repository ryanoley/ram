import numpy as np
import pandas as pd
import datetime as dt


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


def ern_price_anchor(data, offset=1):
    assert offset >= 0, 'Offset must be greater than/equal to 0'
    inds = np.where(data.EARNINGSFLAG == 1)[0] + offset
    inds = inds[inds >= 0]
    anchor = np.zeros(data.shape[0])
    anchor[inds] = 1
    data['anchor'] = anchor
    return data


def anchor_returns(data):
    data['anchor_price'] = np.where(data.anchor == 1, data.AdjClose, np.nan)
    # Transition inds
    inds = data.SecCode != data.SecCode.shift(1)
    data.loc[inds, 'anchor_price'] = -9999
    data['anchor_price'] = data.anchor_price.fillna(method='pad')
    data['anchor_ret'] = data.AdjClose / data.anchor_price - 1
    # Clean inds
    inds = data.anchor_price == -9999
    data.loc[inds, 'anchor_ret'] = np.nan
    # Get rid of blackout rets
    inds = data.blackout == 1
    data.loc[inds, 'anchor_ret'] = np.nan
    data = data.drop(['anchor_price'], axis=1)
    return data
