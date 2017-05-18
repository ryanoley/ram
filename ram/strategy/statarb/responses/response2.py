import numba
import numpy as np
import pandas as pd
import datetime as dt


# ~~~~~~ Response Prototype 2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def response_strategy_2(close1, close2, window, z_open, z_close):
    """
    Strategy is meant to enter on one side, and take the profit when realized
    otherwise close the position at the end of `max_days`.

    The first data-frame represents Long Close2-Short Close1.

    Returns
    -------
    Two data frames that represent the long and the short side
    """
    assert isinstance(close1, pd.DataFrame)
    assert isinstance(close2, pd.DataFrame)
    # Get ZScores
    spreads = np.subtract(np.log(close1), np.log(close2))
    ma = spreads.rolling(window=window).mean()
    std = spreads.rolling(window=window).std()
    zscores = ((spreads - ma) / std).fillna(0).values
    # Create output objects
    column_names = ['{}~{}'.format(x, y)
                    for x, y in zip(close1.columns, close2.columns)]
    output1 = close1.copy()
    output1.columns = column_names
    close1 = close1.values.astype(float)
    output2 = close2.copy()
    output2.columns = column_names
    close2 = close2.values.astype(float)
    responses1 = np.ones(close1.shape) * np.nan
    responses2 = np.ones(close1.shape) * np.nan
    # Ensure this is a  number
    _response_strategy_1(close1, close2, responses1, responses2,
                         zscores, z_open, z_close)
    output1[:] = responses1
    output2[:] = responses2
    return output1, output2


#@numba.jit(nopython=True)
def _response_strategy_2(close1, close2, responses1, responses2,
                         zscores, z_open, z_close):
    n_days, n_pairs = close1.shape
    side = 0
    for c in range(n_pairs):
        for i in range(n_days-1):
            if zscores[i, c] > 1:
                pass
            # Long Close2, Short Close1
            for j in range(i+1, n_days):
                if close2[j, c] / close2[i, c] - \
                        close1[j, c] / close1[i, c] >= take:
                    break
            responses1[i, c] = close2[j, c] / close2[i, c] - \
                close1[j, c] / close1[i, c]

            # Long Close1, Short Close2
            for j in range(i+1, n_days):
                if close1[j, c] / close1[i, c] - \
                        close2[j, c] / close2[i, c] >= take:
                    break

            responses2[i, c] = close1[j, c] / close1[i, c] - \
                close2[j, c] / close2[i, c]
