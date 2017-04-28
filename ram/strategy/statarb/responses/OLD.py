
# ~~~~~~ Optimized Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@numba.jit(nopython=True)
def get_return_series(enter_z, exit_z,
                      zscores, close1, close2,
                      returns):
    """
    Takes in multidimensional zscores, closes1/closes2 etc
    """
    n_days, n_pairs = close1.shape
    for j in xrange(n_pairs):
        side = 0
        for i in xrange(n_days-1):
            # LONGS
            if (side != 1) & (zscores[i, j] >= enter_z):
                side = 1
                returns[i+1, j] = close2[i+1, j] / close2[i, j] - close1[i+1, j] / close1[i, j]
            elif (side == 1) & (zscores[i, j] <= exit_z):
                side = 0
            elif (side == 1):
                returns[i+1, j] = close2[i+1, j] / close2[i, j] - close1[i+1, j] / close1[i, j]

            # SHORTS
            elif (side != -1) & (-zscores[i, j] >= enter_z):
                side = -1
                returns[i+1, j] = close1[i+1, j] / close1[i, j] - close2[i+1, j] / close2[i, j]
            elif (side == -1) & (-zscores[i, j] <= exit_z):
                side = 0
            elif (side == -1):
                returns[i+1, j] = close1[i+1, j] / close1[i, j] - close2[i+1, j] / close2[i, j]


# ~~~~~~ Response Variables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_response_series(close1, close2, stop, max_days):
    responses1 = np.zeros(close1.shape)
    responses2 = np.zeros(close1.shape)
    # Ensure this is a negative number
    stop = -1 * abs(stop)
    return _get_response_series(close1, close2,
                                responses1, responses2,
                                stop, max_days)

# This is more a stop
@numba.jit(nopython=True)
def _get_response_series(close1, close2,
                         responses1, responses2,
                         stop, max_days):
    n_days, n_pairs = close1.shape
    for c in range(n_pairs):
        for i in range(n_days-1):
            # Days forward
            days_forward = (i+max_days+1) if (i+max_days+1) < (n_days-1) \
                else (n_days-1)
            # Long Close2, Short Close1
            for j in range(i+1, days_forward):
                if close2[j, c] / close2[i, c] - \
                        close1[j, c] / close1[i, c] < stop:
                    break
            responses1[i, c] = close2[j, c] / close2[i, c] - \
                close1[j, c] / close1[i, c]

            # Long Close1, Short Close2
            for j in range(i+1, days_forward):
                if close1[j, c] / close1[i, c] - \
                        close2[j, c] / close2[i, c] < stop:
                    break
            responses2[i, c] = close1[j, c] / close1[i, c] - \
                close2[j, c] / close2[i, c]
    return responses1, responses2

# ~~~~~~ Trade Signals ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_trade_signal_series(zscores, close1, close2, enter_z, exit_z):
    signals = np.zeros(zscores.shape)
    _get_trade_signal_series(zscores, close1, close2, enter_z, exit_z, signals)
    return signals


@numba.jit(nopython=True)
def _get_trade_signal_series(zscores, close1, close2,
                             enter_z, exit_z, signals):
    n_days, n_pairs = zscores.shape
    for col in xrange(n_pairs):
        i = 0
        j = 0
        while i < (n_days - 1):
            # LONGS
            if zscores[i, col] >= enter_z:
                j = i + 1
                while True:
                    if (zscores[j, col] <= exit_z) or (j == (n_days - 1)):
                        signals[i, col] = close2[j, col] / close2[i, col] - \
                            close1[j, col] / close1[i, col]
                        break
                    j += 1
                i = j + 1
            # SHORTS
            elif -zscores[i, col] >= enter_z:
                j = i + 1
                while True:
                    if (-zscores[j, col] <= exit_z) or (j == (n_days - 1)):
                        signals[i, col] = close1[j, col] / close1[i, col] - \
                            close2[j, col] / close2[i, col]
                        break
                    j += 1
                i = j + 1
            else:
                i += 1


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_reentry_strategy_rets(zscores, close1, close2, enter_z, exit_z):
    signals = np.zeros(zscores.shape)
    _get_reentry_strategy_rets(zscores, close1, close2, enter_z, exit_z, signals)
    return signals


@numba.jit(nopython=True)
def _get_reentry_strategy_rets(zscores, close1, close2,
                               enter_z, exit_z, signals):
    n_days, n_pairs = zscores.shape
    for col in range(n_pairs):

        trigger = 0
        pos_side = 0
        entry_i = 0

        for i in range(n_days):

            # Check if need to close positions
            if (pos_side == 1) and (zscores[i, col] < exit_z):
                signals[entry_i, col] = close2[i, col] / close2[entry_i, col] - \
                            close1[i, col] / close1[entry_i, col]
                pos_side = 0
                trigger = 0

            elif (pos_side == -1) and (zscores[i, col] > -exit_z):
                signals[entry_i, col] = close1[i, col] / close1[entry_i, col] - \
                            close2[i, col] / close2[entry_i, col]
                pos_side = 0
                trigger = 0

            # Check if price trigger
            if (trigger == 0) and (zscores[i, col] > enter_z):
                trigger = 1
            elif (trigger == 0) and (zscores[i, col] < -enter_z):
                trigger = -1

            # Check if need to open position
            elif (trigger == 1) and (pos_side == 0) and (zscores[i, col] < enter_z):
                pos_side = 1
                entry_i = int(i)
            elif (trigger == -1) and (pos_side == 0)  and (zscores[i, col] > -enter_z):
                pos_side = -1
                entry_i = int(i)

        if pos_side == 1:
            signals[entry_i, col] = close2[i, col] / close2[entry_i, col] - \
                close1[i, col] / close1[entry_i, col]
        elif pos_side == -1:
            signals[entry_i, col] = close1[i, col] / close1[entry_i, col] - \
                        close2[i, col] / close2[entry_i, col]

