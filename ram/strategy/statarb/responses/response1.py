import numba
import numpy as np
import pandas as pd
import datetime as dt


# ~~~~~~ Response Prototype 1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def response_strategy_1(close1, close2, take, max_days):
    """
    Strategy is meant to enter on one side, and take the profit when realized
    otherwise close the position at the end of `max_days`.

    Returns
    -------
    Two data frames that represent the long and the short side
    """
    if isinstance(close1, pd.DataFrame):
        close1 = close1.values.astype(float)
    if isinstance(close2, pd.DataFrame):
        close2 = close2.values.astype(float)
    responses1 = np.ones(close1.shape) * np.nan
    responses2 = np.ones(close1.shape) * np.nan
    # Ensure this is a  number
    return _response_strategy_1(close1, close2,
                                responses1, responses2,
                                abs(float(take)), abs(int(max_days)))


@numba.jit(nopython=True)
def _response_strategy_1(close1, close2,
                         responses1, responses2,
                         take, max_days):
    n_days, n_pairs = close1.shape
    for c in range(n_pairs):
        for i in range(n_days-max_days):
            # Long Close2, Short Close1
            for j in range(i+1, i+max_days+1):
                if close2[j, c] / close2[i, c] - \
                        close1[j, c] / close1[i, c] >= take:
                    break
            responses1[i, c] = close2[j, c] / close2[i, c] - \
                close1[j, c] / close1[i, c]
            # Long Close1, Short Close2
            for j in range(i+1, i+max_days+1):
                if close1[j, c] / close1[i, c] - \
                        close2[j, c] / close2[i, c] >= take:
                    break
            responses2[i, c] = close1[j, c] / close1[i, c] - \
                close2[j, c] / close2[i, c]
    return responses1, responses2


# ~~~~~~ Response Prototype 1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

