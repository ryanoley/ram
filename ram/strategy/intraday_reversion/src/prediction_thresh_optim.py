import numpy as np
from tqdm import tqdm


def prediction_thresh_optim(data,
                            zLim=0.5,
                            gap_down_limit_1=0.25,
                            gap_down_limit_2=0.25,
                            gap_up_limit_1=0.25,
                            gap_up_limit_2=0.25):
    assert 'Date' in data
    assert 'prediction' in data
    assert 'response' in data
    assert 'zOpen' in data

    # Format for fast computation
    data_gap_down = data.loc[data.zOpen < -zLim].copy()
    data_gap_down.sort_values('prediction', inplace=True)

    data_gap_up = data.loc[data.zOpen > zLim].copy()
    data_gap_up.sort_values('prediction', inplace=True)

    eval_dates = np.unique(data.Date)
    eval_dates = eval_dates[eval_dates >= data_gap_down.Date.min()]
    eval_dates = eval_dates[eval_dates >= data_gap_up.Date.min()]

    # Ensure sufficient number of days of training data
    print('\nFitting prediction thresholds:')
    for date in tqdm(eval_dates[50:]):
        inds = data.Date == date

        data.loc[inds, 'gap_down_inflection'] = \
            _get_prediction_thresh(
                data_gap_down.loc[data_gap_down.Date < date],
                gap_down_limit_1, gap_down_limit_2)

        data.loc[inds, 'gap_up_inflection'] = \
            _get_prediction_thresh(
                data_gap_up.loc[data_gap_up.Date < date],
                gap_up_limit_1, gap_up_limit_2)

    return _get_trade_signals(data, zLim)


def _get_prediction_thresh(data,
                           gap_limit_low_side,
                           gap_limit_high_side):
    """
    DATA MUST BE PRE-SORTED BY PREDICTION COLUMN!!
    """
    n_obs = np.arange(1, len(data) + 1, dtype=np.float_)

    win_row_and_below = np.cumsum((data.response == -1).values) / n_obs

    win_above = (np.cumsum((data.response == 1).values[::-1]) / n_obs)[::-1]
    win_above = np.roll(win_above, -1)
    win_above[-1] = np.nan

    wins = win_row_and_below + win_above

    # Trim values from extremes to control odd behavior
    trim_low = int(len(data) * gap_limit_low_side)
    trim_high = int(len(data) * gap_limit_high_side)

    max_ind = np.argmax(wins[trim_low:-trim_high])
    data_ind = data.index[trim_low:-trim_high][max_ind]

    return data.prediction.loc[data_ind]


def _get_trade_signals(data, zLim):
    return np.where(
        # GAP DOWN SHORTS
        (data.prediction <= data.gap_down_inflection) &
        (data.zOpen < -zLim), -1, np.where(
        # GAP DOWN LONGS
        (data.prediction > data.gap_down_inflection) &
        (data.zOpen < -zLim), 1, np.where(
        # GAP UP SHORTS
        (data.prediction <= data.gap_up_inflection) &
        (data.zOpen > zLim), -1, np.where(
        # GAP UP LONGS
        (data.prediction > data.gap_up_inflection) &
        (data.zOpen > zLim), 1,
        # ELSE ZERO
        0))))
