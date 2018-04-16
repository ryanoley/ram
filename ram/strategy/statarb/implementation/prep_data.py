import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from shutil import copyfile

from gearbox import convert_date_array

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.strategy.statarb.version_002.constructor.sizes import SizeContainer

IMP_DIR = config.IMPLEMENTATION_DATA_DIR
RAMEX_DIR = os.path.join(os.getenv('DATA'), 'ramex')


###############################################################################

def clear_live_directory():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live')
    all_files = os.listdir(path)

    # Check meta to confirm wipe of manually_handled_tickers.json
    today = dt.date.today().strftime('%Y%m%d')
    if 'meta.json' in all_files:
        meta = json.load(open(os.path.join(path, 'meta.json'), 'r'))

    # Drop files
    for f in all_files:
        os.remove(os.path.join(path, f))

    meta = {'prepped_date': today}
    json.dump(meta, open(os.path.join(path, 'meta.json'), 'w'))

    if 'handled_bloomberg_tickers.json' not in os.listdir(path):
        json.dump({'_orig': '_new'}, open(os.path.join(
            path, 'handled_bloomberg_tickers.json'), 'w'))

    return


###############################################################################

def get_killed_seccodes():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'killed_seccodes.json')
    return json.load(open(path, 'r')).keys()


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

def copy_version_data(today, killed_seccodes):

    archive_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                               'StatArbStrategy',
                               'archive',
                               'version_data')

    live_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live')

    # Get version and market data, and copy
    prefix = today.strftime('%Y%m%d')
    all_files = os.listdir(archive_dir)
    version_files = [x for x in all_files if x.find(prefix) > -1]

    for v in version_files:
        v_clean = v.replace('{}_'.format(prefix), '')
        data = pd.read_csv(os.path.join(archive_dir, v))
        # Drop some SecCodes
        data.SecCode = data.SecCode.astype(str)
        data = data[~data.SecCode.isin(killed_seccodes)]
        data.to_csv(os.path.join(live_dir, v_clean), index=None)

    return


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


def map_live_tickers(today):
    # Collect message
    output = pd.DataFrame()
    output['Desc'] = ['Ticker Mapping']

    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'archive',
                            'ticker_mapping')
    all_files = os.listdir(data_dir)

    file_name = '{}_ticker_mapping.csv'.format(today.strftime('%Y%m%d'))

    if file_name in all_files:
        output['Message'] = '*'
    else:
        output['Message'] = '[WARNING] - Incorrect Date in Archive'
        return output

    # Format
    data = pd.read_csv(os.path.join(data_dir, file_name))
    data = data[['SecCode', 'Ticker', 'Issuer']]

    # Get hash table for odd tickers
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'qad_to_eze_ticker_map.json')
    odd_tickers = json.load(open(path, 'r'))
    data.Ticker = data.Ticker.replace(to_replace=odd_tickers)

    # Write file to live directory
    new_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live',
                            'eze_tickers.csv')
    data.to_csv(new_path, index=None)

    return output


def check_qad_scaling(today):
    # Collect message
    output = pd.DataFrame()
    output['Desc'] = ['QAD Scaling']

    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'archive',
                            'qad_scaling')
    all_files = os.listdir(data_dir)

    file_name = '{}_seccode_scaling.csv'.format(today.strftime('%Y%m%d'))
    if file_name in all_files:
        output['Message'] = '*'
    else:
        output['Message'] = '[WARNING] - Incorrect Date in Archive'
        return output

    # Copy file to live directory
    # Write file to live directory
    new_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live',
                            'seccode_scaling.csv')

    copyfile(os.path.join(data_dir, file_name), new_path)

    return output


###############################################################################

def check_eod_positions(yesterday):
    positions_path = os.path.join(RAMEX_DIR, 'eod_positions')
    live_path = os.path.join(IMP_DIR,
                             'StatArbStrategy',
                             'live',
                             'eod_positions.csv')

    pos_file_name = '{}_positions.csv'.format(yesterday.strftime('%Y%m%d'))

    if pos_file_name in os.listdir(positions_path):
        message = '*'
        copyfile(os.path.join(positions_path, pos_file_name), live_path)

    else:
        message = '[WARNING] - Missing yesterday\'s file'

    output = pd.DataFrame()
    output.loc[0, 'Desc'] = 'EOD Position File'
    output.loc[0, 'Message'] = message
    return output


###############################################################################

def check_size_containers(yesterday,
                          data_dir=IMP_DIR,
                          models_dir=statarb_config.trained_models_dir_name):
    """
    Checks that correct SizeContainer file exists, handles new size containers
    and copies file to live directory.
    """
    # When new models have been introduced, the SizeContainer needs to be
    # updated
    check_new_sizes(yesterday, data_dir, models_dir)

    # Find SizeContainer files for yesterday
    dpath = os.path.join(data_dir,
                         'StatArbStrategy',
                         'archive',
                         'size_containers')
    all_files = os.listdir(dpath)
    files = [x for x in all_files if x.find(yesterday.strftime('%Y%m%d')) > -1]

    # Infer if new size container exists
    output = pd.DataFrame()
    output.loc[0, 'Desc'] = 'Size containers'

    if len(files) == 0:
        message = '[WARNING] - Wrong File Date Prefix'
        output.loc[0, 'Message'] = message
        return output

    elif len(files) == 2:
        message = 'NOTE: New model SizeContainers being used'
        file_name = [x for x in files if x.find('NEW_MODEL') > -1][0]

    elif len(files) == 1:
        message = '*'
        file_name = files[0]

    else:
        raise ValueError('Size container number of files')

    # Copy target path
    archive_file_path = os.path.join(data_dir,
                                     'StatArbStrategy',
                                     'archive',
                                     'size_containers',
                                     file_name)

    new_file_path = os.path.join(data_dir,
                                 'StatArbStrategy',
                                 'live',
                                 'size_containers.json')

    copyfile(archive_file_path, new_file_path)

    output.loc[0, 'Message'] = message
    return output


def check_new_sizes(yesterday,
                    data_dir=IMP_DIR,
                    models_dir=statarb_config.trained_models_dir_name):
    """
    Check to see if new SizeContainers need to be created from newly
    trained model
    """
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'trained_models',
                        models_dir)

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


###############################################################################

def map_seccodes_bloomberg_tickers(killed_seccodes):

    prefix = dt.date.today().strftime('%Y%m%d')

    # Import Bloomberg Ticker Mapping Files
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'bloomberg_data')

    all_files = os.listdir(data_dir)
    map_files1 = [x for x in all_files if x.find('ticker_cusip.csv') > -1]
    map_files2 = [x for x in all_files if x.find('ticker_cusip2.csv') > -1]

    file_name1 = max(map_files1)
    file_name2 = max(map_files2)

    message = []
    if file_name1.find(prefix) == -1:
        message.append('Map1 Wrong File Date Prefix')
    if file_name2.find(prefix) == -1:
        message.append('Map2 Wrong File Date Prefix')

    data1 = pd.read_csv(os.path.join(data_dir, file_name1))
    data2 = pd.read_csv(os.path.join(data_dir, file_name2))

    # Process Bloomberg Ticker Mapping Files
    data = data1[['CUSIP', 'Ticker']] \
        .append(data2[['CUSIP', 'Ticker']]).reset_index(drop=True)
    data.columns = ['BloombergCusip', 'BloombergId']
    data.BloombergCusip = data.BloombergCusip.astype(str)
    data.BloombergCusip = [x[:8] for x in data.BloombergCusip]
    data['Ticker'] = data.BloombergId.apply(lambda x: x.replace(' US', ''))

    # Import QAD Ticker Mapping
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'archive',
                            'ticker_mapping')
    file_name = max(os.listdir(data_dir))
    qad_map = pd.read_csv(os.path.join(data_dir, file_name))
    qad_map = qad_map[~qad_map.Ticker.isin(['$SPX.X', '$VIX.X'])]
    qad_map = qad_map[~qad_map.SecCode.isin(killed_seccodes)]

    # Merge
    qad_map = qad_map.merge(data, how='left', on='Ticker')

    # Import Odd Ticker HashMap
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy',
                         'qad_to_bbrg_ticker_map.json')
    ticker_map = json.load(open(dpath, 'r'))

    # Fill in manually handled values from ticker_map
    for k, v in ticker_map.iteritems():
        ind = qad_map[qad_map.Ticker == k].index
        if len(ind) > 0:
            qad_map.loc[ind[0], 'BloombergId'] = v + ' US'

    if qad_map.BloombergId.isnull().sum() > 0:
        message.append('Mapping missing data')
        # Write problem file to live directory for debug
        dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                             'StatArbStrategy',
                             'live',
                             'MISSING_BLOOMBERG_ID.csv')
        qad_map[qad_map.BloombergId.isnull()].to_csv(dpath, index=None)

    qad_map = qad_map[['SecCode', 'BloombergId']]

    return qad_map, message


def import_bloomberg_dividends():

    prefix = dt.date.today().strftime('%Y%m%d')

    # Import Bloomberg Ticker Mapping Files
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'bloomberg_data')

    all_files = os.listdir(data_dir)
    div_files = [x for x in all_files if x.find('dividends.csv') > -1]

    file_name = max(div_files)

    message = []
    if file_name.find(prefix) == -1:
        message.append('Wrong File Date Prefix')
        out = pd.DataFrame(columns=['BloombergId', 'DivMultiplier'])
        return out, message

    data = pd.read_csv(os.path.join(data_dir, file_name))

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Dvd Ex Dt'])
    data = data[data.ExDate == dt.date.today()]

    # Calculate DivMultiplier
    data['DivMultiplier'] = data['DPS Last Gross'] / data['Price:D-1'] + 1
    data = data[data.DivMultiplier != 1]

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'DivMultiplier']]

    data = data.reset_index(drop=True).dropna()

    if len(data):
        if np.any(np.abs(data.DivMultiplier - 1) > .1):
            message.append('Spotcheck dividend multiplier')

    return data, message


def import_bloomberg_spinoffs():

    prefix = dt.date.today().strftime('%Y%m%d')

    # Import Bloomberg Ticker Mapping Files
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'bloomberg_data')

    all_files = os.listdir(data_dir)
    spin_files = [x for x in all_files if x.find('_spinoffs.csv') > -1]

    file_name = max(spin_files)

    message = []
    if file_name.find(prefix) == -1:
        message.append('Wrong File Date Prefix')
        out = pd.DataFrame(columns=['BloombergId', 'SpinoffMultiplier'])
        return out, message

    data = pd.read_csv(os.path.join(data_dir, file_name))

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Spinoff Ex Date'])
    data = data[data.ExDate == dt.date.today()]

    # Calculate SpinoffMultiplier
    data['SpinoffMultiplier'] = 1 / data['Spin Adj Fact Curr']

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'SpinoffMultiplier']]

    data = data.reset_index(drop=True).dropna()

    if len(data):
        flags = (data.SpinoffMultiplier < .1) | (data.SpinoffMultiplier > 10)
        if np.any(flags):
            message.append('Spotcheck spinoff multiplier')

    return data, message


def import_bloomberg_splits():

    prefix = dt.date.today().strftime('%Y%m%d')

    # Import Bloomberg Ticker Mapping Files
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'bloomberg_data')

    all_files = os.listdir(data_dir)
    split_files = [x for x in all_files if x.find('_splits.csv') > -1]

    file_name = max(split_files)

    message = []
    if file_name.find(prefix) == -1:
        message.append('Wrong File Date Prefix')
        # RETURN HERE
        out = pd.DataFrame(columns=['BloombergId', 'SplitMultiplier'])
        return out, message

    data = pd.read_csv(os.path.join(data_dir, file_name))

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Stk Splt Ex Dt'])
    data = data[data.ExDate == dt.date.today()]

    # Calculate SpinoffMultiplier
    # Next Stock Split Ratio??
    data['SplitMultiplier'] = data['Current Stock Split Adjustment Factor']

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'SplitMultiplier']]

    data = data.reset_index(drop=True).dropna()

    # Checks
    if len(data):
        if np.any((data.SplitMultiplier < .1) | (data.SplitMultiplier > 10)):
            message.append('Spotcheck split multiplier')

    return data, message


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def process_bloomberg_data(killed_seccodes):

    messages = pd.DataFrame()

    # ID MAPPING
    qad_map, messages_ = map_seccodes_bloomberg_tickers(killed_seccodes)
    messages.loc[0, 'Desc'] = 'Bloomberg QAD Mapping'
    messages.loc[0, 'Message'] = process_messages(messages_)

    # DIVIDENDS
    divs, messages_ = import_bloomberg_dividends()
    messages.loc[1, 'Desc'] = 'Bloomberg Dividends'
    messages.loc[1, 'Message'] = process_messages(messages_)

    # SPINOFFS
    spins, messages_ = import_bloomberg_spinoffs()
    messages.loc[2, 'Desc'] = 'Bloomberg Spinoffs'
    messages.loc[2, 'Message'] = process_messages(messages_)

    # SPLITS
    splits, messages_ = import_bloomberg_splits()
    messages.loc[3, 'Desc'] = 'Bloomberg Splits'
    messages.loc[3, 'Message'] = process_messages(messages_)

    # MERGE
    bloomberg = divs.merge(spins, how='outer') \
        .merge(splits, how='outer').fillna(1)

    # Map SecCodes
    bloomberg = qad_map.merge(bloomberg)

    bloomberg = bloomberg[['SecCode', 'DivMultiplier',
                           'SpinoffMultiplier', 'SplitMultiplier']]

    # Don't write if not complete
    if not np.all(messages.Message == '*'):
        return messages

    # Write bloomberg data to file
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live',
                        'bloomberg_scaling.csv')
    bloomberg.to_csv(path, index=None)

    # Archived
    d = dt.date.today().strftime('%Y%m%d')
    file_name = '{}_bloomberg_scaling.csv'.format(d)
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'archive',
                        'bloomberg_scaling',
                        file_name)
    bloomberg.to_csv(path, index=None)

    return messages


def process_messages(messages_):
    message = '; '.join(messages_)
    if len(message) == 0:
        message = '*'
    else:
        message = '[WARNING] - ' + message
    return message


###############################################################################

def main():

    # Object to gather messages
    messages = pd.DataFrame()

    # CLEAN and write meta
    clear_live_directory()

    killed_seccodes = get_killed_seccodes()

    yesterday, today = get_trading_dates()

    # PREPPED DATA
    copy_version_data(today, killed_seccodes)
    data = get_qadirect_data_info(yesterday)
    messages = messages.append(data)

    data = map_live_tickers(today)
    messages = messages.append(data)

    data = check_qad_scaling(today)
    messages = messages.append(data)

    # POSITION DATA
    data = check_eod_positions(yesterday)
    messages = messages.append(data)

    # SIZE CONTAINERS
    data = check_size_containers(yesterday)
    messages = messages.append(data)

    # BLOOMBERG
    data = process_bloomberg_data(killed_seccodes)
    messages = messages.append(data)

    # Add date column
    messages['Date'] = dt.date.today()

    # OUTPUT to file
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy',
                         'pretrade_data_check.csv')
    messages.to_csv(dpath, index=None)

    prefix = dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy',
                         'archive',
                         'pretrade_checks',
                         '{}_pretrade_data_check.csv'.format(prefix))
    messages.to_csv(dpath, index=None)

    print(messages)


if __name__ == '__main__':
    main()
