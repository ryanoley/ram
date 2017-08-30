import numpy as np
import pandas as pd
import datetime as dt



def make_variable_dict(data, variable, fillna=np.nan):
    data_pivot = data.pivot(index='Date', columns='SecCode', values=variable)
    if fillna == 'pad':
        data_pivot = data_pivot.fillna(method='pad')
    else:
        data_pivot = data_pivot.fillna(fillna)
    return data_pivot.T.to_dict()
