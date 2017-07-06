import numpy as np
import datetime as dt


def get_long_returns(ret_high, ret_low, ret_close,
                     take_perc, stop_perc):
    # Get times at which a trade was triggered
    wins = _get_first_time_index_by_column(ret_high > take_perc)
    losses = _get_first_time_index_by_column(ret_low < -stop_perc)
    return _calculate_rets(wins, losses, ret_close, take_perc, stop_perc)


def get_short_returns(ret_high, ret_low, ret_close,
                      take_perc, stop_perc):
    # Get times at which a trade was triggered
    wins = _get_first_time_index_by_column(ret_low < -take_perc)
    losses = _get_first_time_index_by_column(ret_high > stop_perc)
    # NOTE: sign is flipped on ret_close
    return _calculate_rets(wins, losses, -1 * ret_close, take_perc, stop_perc)


def _get_first_time_index_by_column(data):
    """
    Because this is used in a comparison, and datetime.time cannot be compared
    there is a similar date of 1950-01-01

    Also returns EOD time if value doesn't go above/below threshold
    """
    data.iloc[-1] = True
    tmp = data.unstack().reset_index()
    tmp.columns = ['Date', 'Time', 'Bool']
    tmp = tmp[tmp.Bool]
    times = tmp.groupby('Date')['Time'].first()
    times[:] = [dt.datetime(1950, 1, 1, t.hour, t.minute) for t in times.values]
    return times


def _calculate_rets(wins, losses, ret_close, take_perc, stop_perc):
    rets = wins.copy()
    rets.name = 'Rets'
    rets[:] = np.where(
        # WINNER
        wins < losses, take_perc, np.where(
        # LOSER
        wins > losses, -stop_perc, np.where(
        # MAKES IT TO THE CLOSE
        wins == dt.datetime(1950, 1, 1, 16), ret_close.iloc[-1], np.where(
        losses == dt.datetime(1950, 1, 1, 16), ret_close.iloc[-1], np.where(
        # TIE GETS THE MEAN RETURN
        wins == losses, (take_perc - stop_perc) / 2., 0)))))
    return rets
