import os
import numpy as np
import pandas as pd
import datetime as dt


def basic_model_selection(return_data, window=30, criteria='mean'):
    """
    Will first fill in nan values with zero to do calculations of rolling
    mean or sharpe, but will then force the value down by 99 so it will
    never be selected as among the top models.
    """
    # Rolling mean, offset by one day and select top
    return_data = return_data.copy()
    roll_criteria = return_data.fillna(0).rolling(window=window).mean()
    if criteria == 'sharpe':
        roll_criteria = roll_criteria.divide(return_data.fillna(0).rolling(
            window=window).std())
    roll_criteria = roll_criteria.subtract(
        (return_data.isnull()).astype(int) * 99)
    roll_criteria.fillna(-99, inplace=True)
    roll_criteria.replace(np.inf, -99, inplace=True)
    inds = np.argmax(roll_criteria.values, axis=1)
    best_rets = pd.DataFrame(index=return_data.index)
    best_rets['Rets'] = return_data.values[range(len(return_data)),
                                           np.roll(inds, 1)]
    best_rets.Rets.iloc[:window] = np.nan
    return best_rets
