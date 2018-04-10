import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from shutil import copyfile

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.strategy.statarb.version_002.constructor.sizes import SizeContainer

IMP_DIR = config.IMPLEMENTATION_DATA_DIR
RAMEX_DIR = os.path.join(os.getenv('DATA'), 'ramex')


###############################################################################

def get_trading_dates():
    """
    Returns previous trading date, and current trading date
    """
    today = dt.date.today()
    dh = DataHandlerSQL()
    dates = dh.prior_trading_date([today, today+dt.timedelta(days=1)])
    return dates[0], dates[1]


###############################################################################

def get_qadirect_data_info(yesterday, data_dir=IMP_DIR):
    """
    Get max prefix PER version
    """
    # Search through all files in daily data directory
    data_dir = os.path.join(data_dir,
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


###############################################################################

def check_eod_positions(yesterday, ramex_dir=RAMEX_DIR, data_dir=IMP_DIR):
    path = os.path.join(ramex_dir, 'eod_positions')
    all_files = os.listdir(path)
    file_name = '{}_positions.csv'.format(yesterday.strftime('%Y%m%d'))

    # Copy target path
    live_path = os.path.join(data_dir,
                             'StatArbStrategy',
                             'live',
                             'eod_positions.csv')

    if file_name in all_files:
        message = '*'
        copyfile(os.path.join(path, file_name), live_path)

    else:
        message = '[WARNING] - Missing yesterday\'s file'
        # Remove file
        os.remove(live_path)

    output = pd.DataFrame()
    output.loc[0, 'Desc'] = 'EOD Position File'
    output.loc[0, 'Message'] = message
    return output


###############################################################################

def check_size_containers(yesterday,
                          data_dir=IMP_DIR,
                          models_dir=statarb_config.trained_models_dir_name):

    check_new_sizes(yesterday, data_dir, models_dir)

    dpath = os.path.join(data_dir,
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


def check_new_sizes(yesterday,
                    data_dir=IMP_DIR,
                    models_dir=statarb_config.trained_models_dir_name):
    """
    Check to see if new SizeContainers need to be created from newly
    trained model
    """
    path = os.path.join(data_dir, 'StatArbStrategy',
                        'trained_models', models_dir)

    # Check meta file to see if size containers need to be re-created
    meta = json.load(open(os.path.join(path, 'meta.json'), 'r'))

    if meta['execution_confirm']:
        return

    # Write new files to archive
    all_files = os.listdir(path)
    param_files = [x for x in all_files if x.find('params.json') > -1]

    output = {}
    for f in param_files:
        size_map = json.load(open(os.path.join(path, f), 'r'))
        sc = SizeContainer(-1)
        sc.from_json(size_map['sizes'])
        output[f.replace('_params.json', '')] = sc.to_json()

    prefix = yesterday.strftime('%Y%m%d')
    file_name = '{}_size_containers_NEW_MODEL.json'.format(prefix)
    outpath = os.path.join(data_dir, 'StatArbStrategy', 'archive',
                           'size_containers', file_name)

    json.dump(output, open(outpath, 'w'))

    # Update meta file
    meta['execution_confirm'] = True
    json.dump(meta, open(os.path.join(path, 'meta.json'), 'w'))


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
