import numpy as np
import datetime as dt


def get_long_returns(ret_high, ret_low, ret_close, stop_slippage,
                     transaction_costs, take_perc, stop_perc):
    assert take_perc >= 0
    assert stop_perc >= 0
    # Get times at which a trade was triggered
    wins = _get_first_time_index_by_column(ret_high > take_perc)
    losses = _get_first_time_index_by_column(ret_low < -stop_perc)
    return _calculate_rets(wins, losses, ret_close, stop_slippage,
                           transaction_costs, take_perc, stop_perc)


def get_short_returns(ret_high, ret_low, ret_close, stop_slippage,
                      transaction_costs, take_perc, stop_perc):
    assert take_perc >= 0
    assert stop_perc >= 0
    # Get times at which a trade was triggered
    wins = _get_first_time_index_by_column(ret_low < -take_perc)
    losses = _get_first_time_index_by_column(ret_high > stop_perc)
    # NOTE: sign is flipped on ret_close
    return _calculate_rets(wins, losses, -1 * ret_close, stop_slippage,
                           transaction_costs, take_perc, stop_perc)


def _get_first_time_index_by_column(data):
    """
    Because this is used in a comparison, and datetime.time cannot be compared
    there is a similar date of 1950-01-01

    Also returns EOD time if value doesn't go above/below threshold
    """
    # Append final row if it never crosses the threshold that has
    data = data.copy()
    data.loc[dt.time(23, 59)] = True
    tmp = data.unstack().reset_index()
    tmp.columns = ['Date', 'Time', 'Bool']
    tmp = tmp[tmp.Bool]
    times = tmp.groupby('Date')['Time'].first()
    return times


def _calculate_rets(wins, losses, ret_close, stop_slippage,
                    transaction_costs, take_perc, stop_perc):
    rets = -1 * transaction_costs.copy()
    rets.name = 'Rets'
    # WINNER
    rets += np.where(wins < losses, take_perc, 0) 
    # LOSER
    rets += np.where(wins > losses, -stop_perc - stop_slippage, 0)   
    # MAKES IT TO THE CLOSE
    rets += np.where((wins == losses) & (wins == dt.time(23, 59)),
                        ret_close.iloc[-1], 0)
    # TIE GETS THE MEAN RETURN - DO WE WANT THIS?
    rets += np.where((wins == losses) & (wins != dt.time(23, 59)),
                        (take_perc - stop_perc) / 2., 0)

    return rets

