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


def get_qadirect_data_info(yesterday):
    """
    Get max prefix PER version
    """
    # Search through all files in daily data directory
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy',
                                'daily_data')
    all_files = os.listdir(raw_data_dir)
    # Get unique versions
    data_files = [x for x in all_files if x.find('version_') > -1]
    versions = list(set([x[9:].replace('.csv', '') for x in data_files]))

    output = pd.DataFrame()
    for i, v in enumerate(versions):
        max_file = max([x for x in data_files if x.find(v) > -1])

        # Check date
        data = pd.read_csv(os.path.join(raw_data_dir, max_file), nrows=3000)
        max_date = data.Date.max().split('-')
        max_date = dt.date(int(max_date[0]),
                           int(max_date[1]),
                           int(max_date[2]))
        if max_date != yesterday:
            message = '[WARNING] - max(Date) is not previous trading date'
        else:
            message = '*'

        # Add to output
        output.loc[i, 'Desc'] = 'Data file: ' + max_file
        output.loc[i, 'Message'] = message

    return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():

    yesterday, today = get_trading_dates()

    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'bloomberg_data_check.csv')
    bloomberg = pd.read_csv(dpath)

    qad_data = get_qadirect_data_info(yesterday)

    # Append
    output = bloomberg.append(qad_data).reset_index(drop=True)

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
