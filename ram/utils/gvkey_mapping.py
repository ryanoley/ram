import numpy as np
import pandas as pd
import datetime as dt


def two_gvkeys_one_idccode(data):

    idccode = data.Code.iloc[0]

    min_date, max_date = _min_max_gvkey_dates(data, 'GVKeyChangeDate')

    if (max_date.iloc[0] <= min_date.iloc[1]):
        out = pd.DataFrame()
        out['IdcCode'] = [idccode] * 2
        out['GVKey'] = min_date.index.values
        out['StartDate'] = [dt.datetime(1959, 1, 1), max_date.iloc[0] + dt.timedelta(days=1)]
        out['EndDate'] = [max_date.iloc[0], dt.datetime(2079, 1, 1)]
        return out

    return manually_handle(data)


def one_gvkey_rollup(data):
    idccodes = data.Code.unique()
    out = pd.DataFrame({'IdcCode': idccodes})
    out['GVKey'] = data.GVKey.iloc[0]
    out['StartDate'] = dt.datetime(1959, 1, 1)
    out['EndDate'] = dt.datetime(2079, 1, 1)
    return out


def _min_max_gvkey_dates(data, column):
    min_date = data.groupby('GVKey')[column].min()
    min_date.sort_values(inplace=True)
    max_date = data.groupby('GVKey')[column].max()
    max_date.sort_values(inplace=True)
    return min_date, max_date


def manually_handle(data):
    X = 10
    return None
