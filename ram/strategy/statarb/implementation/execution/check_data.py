import os
import numpy as np
import pandas as pd
import datetime as dt
from shutil import copyfile

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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_qadirect_data_info(yesterday):
    """
    Get max prefix PER version
    """
    # Search through all files in daily data directory
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live')
    all_files = os.listdir(data_dir)
    # Get unique versions
    version_files = [x for x in all_files if x.find('version_') > -1]

    output = pd.DataFrame()
    for i, v in enumerate(version_files):
        # Check date
        data = pd.read_csv(os.path.join(data_dir, v), nrows=3000)
        max_date = data.Date.max().split('-')
        max_date = dt.date(int(max_date[0]),
                           int(max_date[1]),
                           int(max_date[2]))
        if max_date != yesterday:
            message = '[WARNING] - max(Date) is not previous trading date'
        else:
            message = '*'

        # Add to output
        output.loc[i, 'Desc'] = 'Data file: ' + v
        output.loc[i, 'Message'] = message

    return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def check_eod_positions(yesterday):
    dpath = os.path.join(os.getenv('DATA'),
                         'ramex',
                         'eod_positions')
    all_files = os.listdir(dpath)
    file_name = '{}_positions.csv'.format(yesterday.strftime('%Y%m%d'))

    # Copy target path
    live_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                             'StatArbStrategy',
                             'live',
                             'eod_positions.csv')

    if file_name in all_files:
        message = '*'
        copyfile(os.path.join(dpath, file_name), live_path)

    else:
        message = '[WARNING] - Missing yesterday\'s file'
        # Remove file
        os.remove(live_path)

    output = pd.DataFrame()
    output.loc[0, 'Desc'] = 'EOD Position File'
    output.loc[0, 'Message'] = message
    return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def check_size_containers(yesterday):
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy',
                         'archive',
                         'size_containers')

    all_files = os.listdir(dpath)

    file_name = '{}_size_containers.json'.format(yesterday.strftime('%Y%m%d'))

    # Copy target path
    live_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                             'StatArbStrategy',
                             'live',
                             'size_containers.json')

    if file_name in all_files:
        message = '*'
        copyfile(os.path.join(dpath, file_name), live_path)

    else:
        message = '[WARNING] - Missing yesterday\'s file'
        # Remove file
        os.remove(live_path)

    output = pd.DataFrame()
    output.loc[0, 'Desc'] = 'Size containers'
    output.loc[0, 'Message'] = message
    return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():

    yesterday, today = get_trading_dates()

    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'bloomberg_data_check.csv')
    bloomberg = pd.read_csv(dpath)

    qad_data = get_qadirect_data_info(yesterday)

    position_file = check_eod_positions(yesterday)

    size_containers = check_size_containers(yesterday)

    # Append
    output = qad_data.append(position_file) \
        .append(size_containers).reset_index(drop=True)
    # Add date column
    output['Date'] = dt.date.today()
    output = bloomberg.append(output)

    # OUTPUT to file
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'pretrade_data_check.csv')
    output.to_csv(dpath, index=None)


if __name__ == '__main__':
    main()
