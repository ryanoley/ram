import os
import numpy as np
import pandas as pd
import datetime as dt

from ram import config
from ram.data.data_handler_sql import DataHandlerSQL

from gearbox import convert_date_array


def get_trading_dates():
    """
    Returns previous trading date, and current trading date
    """
    today = dt.date.today()
    dh = DataHandlerSQL()
    dates = dh.prior_trading_date([today, today+dt.timedelta(days=1)])
    return dates[0], dates[1]


def get_bloomberg_file_prefix_date(file_name):
    bloomberg_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                 'bloomberg_data')
    all_files = os.listdir(bloomberg_dir)
    all_files = [x for x in all_files if x.find(file_name) > -1]
    prefix = max([x.split('_')[0] for x in all_files])
    return dt.date(int(prefix[:4]), int(prefix[4:6]), int(prefix[6:]))


def get_qadirect_file_prefix_dates():
    # QADIRECT - also cleans out old data
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    all_files.remove('market_index_data.csv')
    prefix = max([x.split('_')[0] for x in all_files])
    return dt.date(int(prefix[:4]), int(prefix[4:6]), int(prefix[6:]))


def get_qadirect_data_dates():
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    all_files.remove('market_index_data.csv')
    max_date_prefix = max([x.split('_')[0] for x in all_files])
    # Read in dates for files
    todays_files = [x for x in all_files if x.find(max_date_prefix) > -1]
    max_dates = []
    for f in todays_files:
        data = pd.read_csv(os.path.join(raw_data_dir, f), nrows=3000)
        max_dates.append(data.Date.max())
    return todays_files, convert_date_array(max_dates)


def main():
    yesterday, today = get_trading_dates()

    output = pd.DataFrame()

    output.loc[0, 'Desc'] = 'Bloomberg Dividend File Prefix'
    date = get_bloomberg_file_prefix_date('dividends')
    output.loc[0, 'Date'] = date
    output.loc[0, 'Alert'] = '!!!' if date != today else ''

    output.loc[1, 'Desc'] = 'Bloomberg Spinoff File Prefix'
    date = get_bloomberg_file_prefix_date('spinoffs')
    output.loc[1, 'Date'] = date
    output.loc[1, 'Alert'] = '!!!' if date != today else ''

    output.loc[2, 'Desc'] = 'Bloomberg Splits File Prefix'
    date = get_bloomberg_file_prefix_date('splits')
    output.loc[2, 'Date'] = date
    output.loc[2, 'Alert'] = '!!!' if date != today else ''

    output.loc[3, 'Desc'] = 'QADirect File Prefix'
    date = get_qadirect_file_prefix_dates()
    output.loc[3, 'Date'] = date
    output.loc[3, 'Alert'] = '!!!' if date != today else ''

    ind = len(output)

    for n, d in zip(*get_qadirect_data_dates()):
        output.loc[ind, 'Desc'] = n
        output.loc[ind, 'Date'] = d
        output.loc[ind, 'Alert'] = '!!!' if d != yesterday else ''
        ind += 1

    # OUTPUT to file
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'pretrade_data_check.csv')
    output.to_csv(dpath, index=None)
    # Archive
    ddir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'pretrade_check_archive')
    if not os.path.isdir(ddir):
        os.mkdir(ddir)
    dpath = os.path.join(ddir, 'pretrade_data_check_{}.csv'.format(
        today.strftime('%Y%m%d')))
    output.to_csv(dpath, index=None)


if __name__ == '__main__':
    main()
