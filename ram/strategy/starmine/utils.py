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



def pull_spy_data(output_path, tickers=['SPY']):
    dh = DataHandlerSQL()
    start_date = dt.datetime(1999,1,1,0,0)
    end_date = dt.datetime(2017,9,1,0,0)
    features = ['SplitFactor', 'RVwap', 'RClose', 'RCashDividend',
                'AdjClose', 'AdjOpen', 'AdjVwap',
                'LEAD1_AdjVwap', 'LEAD11_AdjVwap', 'LEAD21_AdjVwap',
                'LEAD31_AdjVwap', 'LEAD41_AdjVwap']
    
    spy_data = dh.get_etf_data(tickers, features, start_date, end_date)
    spy_data.to_csv(output_path, index=False)
    return

