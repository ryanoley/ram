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


def get_qadirect_data_dates():
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    all_files.remove('market_index_data.csv')
    max_date_prefix = max([x.split('_')[0] for x in all_files])

    # Read in dates for files
    todays_files = [x for x in all_files if x.find('version') > -1]
    todays_files = [x for x in todays_files if x.find(max_date_prefix) > -1]
    max_dates = []
    for f in todays_files:
        data = pd.read_csv(os.path.join(raw_data_dir, f), nrows=3000)
        max_dates.append(data.Date.max())
    return todays_files, convert_date_array(max_dates)


def _check_date(date, today):
    return '[WARNING] - Not up-to-date' if date != today else '*'


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():

    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'bloomberg_data_check.csv')
    bloomberg = pd.read_csv(dpath)

    yesterday, today = get_trading_dates()

    # Check data files
    output = pd.DataFrame()
    ind = len(output)
    for desc, last_date in zip(*get_qadirect_data_dates()):
        desc = 'Raw data last date: ' + desc[desc.find('version'):]
        output.loc[ind, 'Desc'] = desc
        output.loc[ind, 'Message'] = _check_date(last_date, yesterday)
        ind += 1

    # Append
    output = bloomberg.append(output).reset_index(drop=True)

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
