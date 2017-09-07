import numpy as np
import pandas as pd
import datetime as dt

from ram.data.data_handler_sql import DataHandlerSQL


def make_variable_dict(data, variable, fillna=np.nan):
    data_pivot = data.pivot(index='Date', columns='SecCode', values=variable)
    if fillna == 'pad':
        data_pivot = data_pivot.fillna(method='pad')
    else:
        data_pivot = data_pivot.fillna(fillna)
    return data_pivot.T.to_dict()



def get_prior_business_date(unique_test_dates):
    dh = DataHandlerSQL()
    if type(unique_test_dates) is np.ndarray:
        unique_test_dates = unique_test_dates.tolist()
    prior_trading_dts = dh.prior_trading_date(unique_test_dates)
    return prior_trading_dts
