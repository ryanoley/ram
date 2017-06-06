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
    data = ern_date_label(data)
    # Shift for window price
    data['shift_price'] = data.AdjClose.shift(window-1)
    data.shift_price = np.where(data.SecCode != data.SecCode.shift(window+init_offset-1),
                                np.nan, data.shift_price)
    data.shift_price = np.where(data.ern_num != data.ern_num.shift(window+init_offset-1),
                                np.nan, data.shift_price)
    data.shift_price = np.where(data.ern_num == 0, np.nan, data.shift_price)
    # Start of init/trailing Anchor
    init_anchor = data.EARNINGSFLAG.shift(init_offset).fillna(0)

    data['anchor_price'] = np.where(init_anchor, data.AdjClose, np.nan)
    data.anchor_price = np.where(data.anchor_price.isnull(),
                                 data.shift_price, data.anchor_price)

    # Create -9999 at start of new security and start of blackout period
    data.anchor_price = np.where(data.blackout.diff() == 1,
                                 -9999, data.anchor_price)
    data.anchor_price = np.where(data.SecCode != data.SecCode.shift(1),
                                 -9999, data.anchor_price)

    data.anchor_price = data.anchor_price.fillna(method='pad')
    data.anchor_price = data.anchor_price.replace(-9999, np.nan)
    data.anchor_price = np.where(data.blackout, np.nan, data.anchor_price)
    data['anchor_ret'] = data.AdjClose / data.anchor_price
    return data.drop(['shift_price', 'ern_num'], axis=1)


def ern_date_label(data):
    """
    Used to get the number of earnings announcements prior to or including
    the current row specifically for a SecCode
    """
    data['ern_num'] = data.EARNINGSFLAG.cumsum()
    # Transition to new SecCode
    inds = (data.SecCode != data.SecCode.shift(1)).values
    adjs = np.zeros(inds.shape)
    adjs[inds] = data.ern_num.iloc[inds]
    data.ern_num = data.ern_num - np.maximum.accumulate(adjs)
    return data
