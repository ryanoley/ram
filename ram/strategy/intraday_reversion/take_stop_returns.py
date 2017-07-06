import numpy as np


def get_long_returns(ret_high, ret_low, ret_close,
                     take_perc, stop_perc):
    # Get times at which a trade was triggered
    wins = _get_first_time_index_by_column(ret_high > take_perc)
    losses = _get_first_time_index_by_column(ret_low < -stop_perc)
    return _calculate_rets(wins, losses, take_perc, stop_perc)


def get_short_returns(ret_high, ret_low, ret_close,
                      take_perc, stop_perc):
    # Get times at which a trade was triggered
    wins = _get_first_time_index_by_column(ret_low < -take_perc)
    losses = _get_first_time_index_by_column(ret_high > stop_perc)
    return _calculate_rets(wins, losses, take_perc, stop_perc)


def _get_first_time_index_by_column(data):
    """
    Also returns final time if it doesn't hit
    """
    data.iloc[-1] = True
    tmp = data.unstack().reset_index()
    tmp.columns = ['Date', 'Time', 'Bool']
    tmp = tmp[tmp.Bool]
    times = tmp.groupby('Date')['Time'].first()
    times[:] = [dt.datetime(1950, 1, 1, t.hour, t.minute) for t in times.values]
    return times


def _calculate_rets(wins, losses, take_perc, stop_perc):
    rets = wins.copy()
    rets[:] = np.where(
        wins < losses, take_perc, np.where(
        wins > losses, -stop_perc, np.where(
        wins == dt.datetime(1950, 1, 1, 16), (take_perc - stop_perc)/2., 0)))
    return rets
