import os
import pandas as pd

INTRADAY_DIR = os.path.join(os.getenv('DATA'), 'ram', 'intraday_src')


def get_intraday_rets_data(ticker, intraday_dir=INTRADAY_DIR):
    data = _import_data('IWM', intraday_dir)
    return _format_returns(data)


def _import_data(ticker, intraday_dir):
    data = pd.read_csv(os.path.join(intraday_dir, '{}.csv'.format(ticker)))
    data.rename(columns={'Volume': 'CumVolume'}, inplace=True)
    data.DateTime = pd.to_datetime(data.DateTime)
    data['Date'] = data.DateTime.apply(lambda x: x.date())
    data['Time'] = data.DateTime.apply(lambda x: x.time())
    # Remove first day since some seem incomplete
    data = data[data.Date > data.Date.min()]
    return data


def _format_returns(data):
    data_open = _pivot_data(data, 'Open')
    data_high = _pivot_data(data, 'High')
    data_low = _pivot_data(data, 'Low')
    data_close = _pivot_data(data, 'Close')
    # Returns
    ret_high = data_high / data_open.iloc[0] - 1
    ret_low = data_low / data_open.iloc[0] - 1
    ret_close = data_close / data_open.iloc[0] - 1
    return ret_high, ret_low, ret_close


def _pivot_data(data, values):
    return data.pivot(index='Time', columns='Date',
                      values=values).fillna(method='pad')
