import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from shutil import copyfile, rmtree

from gearbox import convert_date_array

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.strategy.statarb.objects.sizes import SizeContainer

IMP_DIR = config.IMPLEMENTATION_DATA_DIR
RAMEX_DIR = os.path.join(os.getenv('DATA'), 'ramex')
STRATEGYID = 'StatArb0001'
HEDGE_TICKERS = ['VOO']


###############################################################################

def clear_live_directory():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live')
    all_files = os.listdir(path)

    # Drop files
    for f in all_files:
        os.remove(os.path.join(path, f))

    today = dt.date.today().strftime('%Y%m%d')
    meta = {
        'prepped_date': today,
        'trained_models_dir_name': statarb_config.trained_models_dir_name
    }
    json.dump(meta, open(os.path.join(path, 'meta.json'), 'w'))
    return


###############################################################################

def get_killed_seccodes():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'killed_seccodes.json')
    # Be sure they are strings
    return [str(x) for x in json.load(open(path, 'r')).keys()]


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
        data = pd.read_csv(os.path.join(data_dir, v))
        max_date = data.Date.max().split('-')
        max_date = dt.date(int(max_date[0]),
                           int(max_date[1]),
                           int(max_date[2]))
        if max_date != yesterday:
            message = '[ERROR] - max(Date) is not previous trading date'

        # Check for missing data
        elif np.any(data.isnull().mean() > 0.2):
            count = sum(data.isnull().mean() > 0.2)
            message = '[ERROR] - {} variables with many '.format(count) + \
                'missing values'
        else:
            message = '*'

        # Add to output
        output.loc[i, 'Desc'] = 'Data file: ' + v
        output.loc[i, 'Message'] = message

    return output


###############################################################################

def map_live_tickers(killed_seccodes):
    # Collect message
    output = pd.DataFrame()
    output['Desc'] = ['Ticker Mapping']

    # Check version data exists
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live')
    all_files = os.listdir(data_dir)
    # Get unique versions
    version_files = [x for x in all_files if x.find('version_') > -1]

    if len(version_files) == 0:
        output['Message'] = '[ERROR] - No version data in directory'
        return output

    else:
        output['Message'] = '*'

    # Check that size container is available
    if not os.path.isfile(os.path.join(data_dir, 'size_containers.json')):
        output['Message'] = '[ERROR] - No size_containers.json'
        return output

    # Get SecCodes
    seccodes1 = get_unique_seccodes_from_data(version_files)
    seccodes2 = get_unique_seccodes_from_size_containers()

    unique_seccodes = list(set(seccodes1 + seccodes2))
    data = get_seccode_ticker_mapping(unique_seccodes)

    if np.any(data.Ticker.isnull()):
        dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                             'StatArbStrategy',
                             'live',
                             'MISSING_QAD_TICKER.csv')
        data[data.Ticker.isnull()].to_csv(dpath, index=None)
        output['Message'] = '[ERROR] - SecCode missing Ticker in QADirect'
        return output

    # Ticker mapping for Bloomberg
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'ticker_changes.json')
    ticker_changes = json.load(open(path, 'r'))
    data.Ticker = data.Ticker.replace(ticker_changes)

    # Ticker mapping for Bloomberg
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live',
                        'qad_seccode_data.csv')
    data.to_csv(path, index=None)

    # Archive
    file_name = '{}_qad_seccode_data.csv'.format(
        dt.date.today().strftime('%Y%m%d'))
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'archive',
                        'qad_seccode_data',
                        file_name)
    data.to_csv(path, index=None)

    # Replace QAD Tickers with EzeTickers
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'qad_to_eze_ticker_map.json')
    eze_tickers = json.load(open(path, 'r'))
    data.Ticker = data.Ticker.replace(eze_tickers)

    # Kill list
    data = data[~data.SecCode.isin(killed_seccodes)]

    # Write file to live directory
    new_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live',
                            'tickers_for_live_pricing.csv')
    data = data[['SecCode', 'Ticker', 'Issuer']]
    data.to_csv(new_path, index=None)

    return output


def get_seccode_ticker_mapping(unique_seccodes):
    """
    Maps Tickers to SecCodes, and writes in archive and in live_pricing
    directory.
    """
    unique_seccodes = pd.DataFrame({'SecCode': unique_seccodes})

    dh = DataHandlerSQL()
    mapping = dh.get_live_seccode_ticker_map()
    mapping = mapping.merge(unique_seccodes, how='right')

    # Hard-coded SecCodes for indexes
    indexes = pd.DataFrame()
    indexes['SecCode'] = ['50311', '11113']
    indexes['Ticker'] = ['$SPX.X', '$VIX.X']
    indexes['Cusip'] = [np.nan, np.nan]
    indexes['Issuer'] = ['SPX', 'VIX']

    mapping = mapping.append(indexes).reset_index(drop=True)

    return mapping


def get_unique_seccodes_from_data(version_files):
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live')
    seccodes = []
    for f in version_files:
        data = pd.read_csv(os.path.join(data_dir, f))
        seccodes += data.SecCode.astype(str).unique().tolist()
    return list(set(seccodes))


def get_unique_seccodes_from_size_containers():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live',
                        'size_containers.json')
    sizes = json.load(open(path, 'r'))
    seccodes = []
    for x in sizes.values():
        for y in x['sizes'].values():
            seccodes += y.keys()
    return list(set(seccodes))


###############################################################################

def check_qad_scaling():
    # Collect message
    output = pd.DataFrame()
    output['Desc'] = ['QAD Scaling']

    # Check if ticker mapping file exists
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live',
                        'qad_seccode_data.csv')

    if os.path.isfile(path):
        output['Message'] = '*'
    else:
        output['Message'] = '[ERROR] - Missing qad_seccode file in live dir'
        return output

    # Get SecCodes
    data = pd.read_csv(path)
    unique_seccodes = data.SecCode.values

    # Write file to archive directory
    dh = DataHandlerSQL()
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=7)
    scaling = dh.get_seccode_data(seccodes=unique_seccodes,
                                  features=['DividendFactor', 'SplitFactor'],
                                  start_date=start_date,
                                  end_date=end_date)
    scaling = scaling[scaling.Date == scaling.Date.max()]

    today = dt.datetime.utcnow()
    file_name = '{}_{}'.format(today.strftime('%Y%m%d'), 'seccode_scaling.csv')
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'archive',
                        'qad_scaling',
                        file_name)

    scaling.to_csv(path, index=None)

    # Write file to live directory
    new_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'live',
                            'seccode_scaling.csv')
    scaling.to_csv(new_path, index=None)

    return output


###############################################################################

def check_eod_positions(yesterday, killed_seccodes):
    positions_path = os.path.join(RAMEX_DIR, 'eod_positions')
    live_path = os.path.join(IMP_DIR,
                             'StatArbStrategy',
                             'live',
                             'eod_positions.csv')

    pos_file_name = '{}_positions.csv'.format(yesterday.strftime('%Y%m%d'))

    if pos_file_name in os.listdir(positions_path):
        message = '*'
        df = pd.read_csv(os.path.join(positions_path, pos_file_name))
        df = df[df.StrategyID == STRATEGYID]
        df = df[~df.Ticker.isin(HEDGE_TICKERS)]
        df.SecCode = df.SecCode.astype(int).astype(str)
        df = df[~df.SecCode.isin(killed_seccodes)]
        df.to_csv(live_path, index=None)

    else:
        message = '[ERROR] - Missing yesterday\'s file'

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
        message = '[ERROR] - Wrong File Date Prefix'
        output.loc[0, 'Message'] = message
        return output

    elif len(files) == 2:
        message = '[INFO] New model SizeContainers being used'
        file_name = [x for x in files if x.find('NEW_MODEL') > -1][0]

    elif len(files) == 1:
        message = '*'
        file_name = files[0]

    else:
        raise ValueError('Size container number of files')

    # Kill list
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'killed_seccodes.json')
    killed_seccodes = json.load(open(path, 'r'))
    killed_seccodes = killed_seccodes.keys()

    # SizeContainer
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'archive',
                        'size_containers',
                        file_name)
    sizes = json.load(open(path, 'r'))

    # OPEN AND KILL
    bad_dates_flag = False
    new_sizes = {}
    for k, v in sizes.iteritems():
        sc = SizeContainer(-1)
        sc.from_json(v)

        # Check dates
        if max(sc.sizes.keys()) != yesterday:
            bad_dates_flag = True

        # KILL
        for seccode in killed_seccodes:
            sc.kill_seccode(seccode)
        new_sizes[k] = sc.to_json()

    if bad_dates_flag:
        output.loc[0, 'Message'] = '[ERROR] SizeContainer has wrong dates'
        return output

    # Write
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'live',
                        'size_containers.json')

    json.dump(new_sizes, open(path, 'w'))

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
    map_files3 = [x for x in all_files if x.find('ticker_cusip3.csv') > -1]

    file_name1 = max(map_files1)
    file_name2 = max(map_files2)
    file_name3 = max(map_files3)

    message = []
    if file_name1.find(prefix) == -1:
        message.append('[ERROR] Map1 Wrong File Date Prefix')
    if file_name2.find(prefix) == -1:
        message.append('[ERROR] Map2 Wrong File Date Prefix')
    if file_name3.find(prefix) == -1:
        message.append('[ERROR] Map3 Wrong File Date Prefix')

    data1 = pd.read_csv(os.path.join(data_dir, file_name1))
    data2 = pd.read_csv(os.path.join(data_dir, file_name2))
    data3 = pd.read_csv(os.path.join(data_dir, file_name3))

    # Process Bloomberg Ticker Mapping Files
    data = data1[['CUSIP', 'Ticker']] \
        .append(data2[['CUSIP', 'Ticker']]) \
        .append(data3[['CUSIP', 'Ticker']]).reset_index(drop=True)
    data.columns = ['BloombergCusip', 'BloombergId']
    data.BloombergCusip = data.BloombergCusip.astype(str)
    data.BloombergCusip = [x[:8] for x in data.BloombergCusip]
    data['Ticker'] = data.BloombergId.apply(lambda x: x.replace(' US', ''))

    # Import QAD Ticker Mapping
    data_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                             'StatArbStrategy',
                             'live',
                             'qad_seccode_data.csv')
    if not os.path.isfile(data_path):
        return pd.DataFrame([]), ['[ERROR] No QAD Ticker Mapping csv']

    qad_map = pd.read_csv(data_path)
    qad_map = qad_map[~qad_map.Ticker.isin(['$SPX.X', '$VIX.X'])]
    qad_map = qad_map[~qad_map.SecCode.isin(killed_seccodes)]

    # Merge
    qad_map = qad_map.merge(data, how='left', on='Ticker')

    # Import Odd Ticker HashMap
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'qad_to_bbrg_ticker_map.json')
    ticker_map = json.load(open(dpath, 'r'))

    # Fill in manually handled values from ticker_map
    for k, v in ticker_map.iteritems():
        ind = qad_map[qad_map.Ticker == k].index
        if len(ind) > 0:
            qad_map.loc[ind[0], 'BloombergId'] = v + ' US'

    if qad_map.BloombergId.isnull().sum() > 0:
        message.append('[ERROR] Missing Bloomberg ticker data')
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
        message.append('[ERROR] Wrong File Date Prefix')
        out = pd.DataFrame(columns=['BloombergId',
                                    'DivMultiplier',
                                    'DivValue'])
        return out, message

    data = pd.read_csv(os.path.join(data_dir, file_name))

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Dvd Ex Dt'])
    data = data[data.ExDate == dt.date.today()]

    # Create Dividend Value columns
    data['DivValue'] = data['DPS Last Gross']

    # Calculate DivMultiplier
    data['DivMultiplier'] = data['DPS Last Gross'] / data['Price:D-1'] + 1
    data = data[data.DivMultiplier != 1]

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'DivMultiplier', 'DivValue']]

    data = data.reset_index(drop=True).dropna()

    if len(data):
        if np.any(np.abs(data.DivMultiplier - 1) > .1):
            message.append('[INFO] Spotcheck dividend multiplier')

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
        message.append('[ERROR] Wrong File Date Prefix')
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
            message.append('[INFO] Spotcheck spinoff multiplier')

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
        message.append('[ERROR] Wrong File Date Prefix')
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
            message.append('[INFO] Spotcheck split multiplier')

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

    # Don't write if not complete
    if np.any(messages.Message.apply(lambda x: x.find('ERROR') > -1)):
        return messages

    # MERGE
    bloomberg = divs.merge(spins, how='outer').merge(splits, how='outer')

    bloomberg.DivMultiplier = bloomberg.DivMultiplier.fillna(1)
    bloomberg.DivValue = bloomberg.DivValue.fillna(0)
    bloomberg.SpinoffMultiplier = bloomberg.SpinoffMultiplier.fillna(1)
    bloomberg.SplitMultiplier = bloomberg.SplitMultiplier.fillna(1)

    # Map SecCodes
    bloomberg = qad_map.merge(bloomberg)

    bloomberg = bloomberg[['SecCode', 'DivMultiplier', 'DivValue',
                           'SpinoffMultiplier', 'SplitMultiplier']]

    # Get EOD position files to see if anything changed for our positions
    # today
    live_path = os.path.join(IMP_DIR,
                             'StatArbStrategy',
                             'live',
                             'eod_positions.csv')

    if os.path.isfile(live_path):
        eod_positions = pd.read_csv(live_path)
    else:
        eod_positions = pd.DataFrame(columns=['SecCode'])

    corp_actions = bloomberg[bloomberg.SecCode.isin(eod_positions.SecCode)]
    if len(corp_actions) > 0:
        message = '[INFO] - {} Corporate Action(s)'.format(len(corp_actions))
        # Write to file
        output = corp_actions.merge(eod_positions)
        output = output[['SecCode', 'Ticker', 'Shares', 'RAMID',
                         'DivMultiplier', 'DivValue', 'SpinoffMultiplier',
                         'SplitMultiplier']]
        path = os.path.join(IMP_DIR,
                            'StatArbStrategy',
                            'live',
                            'POSITION_SHEET_CORPORATE_ACTIONS.csv')
        output.to_csv(path, index=None)
    else:
        message = '*'

    # Drop DivValue as it isn't needed in system
    bloomberg = bloomberg.drop('DivValue', axis=1)

    messages.loc[4, 'Desc'] = 'Position Sheet Corporate Actions'
    messages.loc[4, 'Message'] = message

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
    if len(messages_) == 1:
        return messages_[0]
    message = '; '.join(messages_)
    if len(message) == 0:
        message = '*'
    return message


###############################################################################

def get_short_sell_killed_seccodes(today, rate_min=-5., data_dir=IMP_DIR):
    # Search for todays file
    dt_str = '{d.month}.{d.day}.{d:%y}'.format(d=today)

    locates_dir = os.path.join(data_dir,
                               'StatArbStrategy',
                               'archive',
                               'locates')
    all_locate_files = os.listdir(locates_dir)
    fl = [x for x in all_locate_files if x.find(dt_str) > -1]

    output = pd.DataFrame()
    output.loc[0, 'Desc'] = 'Short Locates'

    if len(fl) == 0:
        message = '[ERROR] no locate file for {} found'.format(dt_str)
        output.loc[0, 'Message'] = message
        return output
    elif len(fl) > 1:
        message = '[ERROR] multiple locate files for {} found'.format(dt_str)
        output.loc[0, 'Message'] = message
        return output

    # Read in file and process
    htb = pd.read_excel(os.path.join(locates_dir, fl[0]))
    htb.drop('Confirmation', axis=1, inplace=True)
    htb.rename(columns={
               'Rate %': 'rate',
               'Rqst Qty': 'req_qty',
               'Approv Qty': 'apr_qty',
               'Status': 'status'},
               inplace=True)
    htb['Security'] = [x[:8] if len(x) > 8 else x for x in htb.Security]
    fltr_rate = np.array(htb.rate < rate_min)
    fltr_appr = np.array(htb.status != 'Approved')
    htb = htb[fltr_rate | fltr_appr].reset_index(drop=True)

    # Merge with live mapping file to get SecCodes
    map_path = os.path.join(data_dir,
                            'StatArbStrategy',
                            'live',
                            'qad_seccode_data.csv')
    map_data = pd.read_csv(map_path)
    map_data.SecCode = map_data.SecCode.astype(int).astype(str)

    tkr_map = map_data[['SecCode', 'Ticker']].copy()
    tkr_map.rename(columns={'SecCode': 'tkr_SecCode'}, inplace=True)

    csp_map = map_data[['SecCode', 'Cusip']].copy()
    csp_map.rename(columns={'SecCode': 'csp_SecCode'}, inplace=True)

    htb = pd.merge(htb, tkr_map, how='left', left_on='Security',
                   right_on='Ticker')
    htb = pd.merge(htb, csp_map, how='left', left_on='Security',
                   right_on='Cusip')

    htb['SecCode'] = [x if pd.notnull(x) else y for x, y in
                      zip(htb.tkr_SecCode, htb.csp_SecCode)]
    htb = htb[['Security', 'rate', 'req_qty', 'apr_qty', 'status', 'SecCode']]
    unmapped = htb.SecCode.isnull().sum()

    # Write to File
    path = os.path.join(data_dir,
                        'short_sell_kill_list.csv')
    htb.to_csv(path, index=None)

    # Add to output
    if unmapped > 0:
        message = '[INFO] {} securities no map to SecCodes'.format(unmapped)
    else:
        message = '*'
    output.loc[0, 'Message'] = message

    return output


###############################################################################

def archive_live_directory():
    live_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                             'StatArbStrategy',
                             'live')
    all_files = os.listdir(live_path)
    # Archive entire directory
    dir_name = '{}_live'.format(dt.date.today().strftime('%Y%m%d'))
    archive_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy',
                                'archive',
                                'live_directories',
                                dir_name)
    # Remove directory if already exists
    if os.path.isdir(archive_path):
        rmtree(archive_path)
    os.mkdir(archive_path)
    # Copy files
    for f in all_files:
        copyfile(os.path.join(live_path, f), os.path.join(archive_path, f))
    # AND copy position file
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'position_size.json')
    copyfile(path, os.path.join(archive_path, 'position_size.json'))
    return


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

    # SIZE CONTAINERS
    data = check_size_containers(yesterday)
    messages = messages.append(data)

    # PULL TICKERS
    data = map_live_tickers(killed_seccodes)
    messages = messages.append(data)

    # PULL SCALING DATA
    data = check_qad_scaling()
    messages = messages.append(data)

    # POSITION DATA
    data = check_eod_positions(yesterday, killed_seccodes)
    messages = messages.append(data)

    # BLOOMBERG
    data = process_bloomberg_data(killed_seccodes)
    messages = messages.append(data)

    # SHORT LOCATES
    data = get_short_sell_killed_seccodes(today, rate_min=-5.)
    messages = messages.append(data)

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

    archive_live_directory()

    print(messages.reset_index(drop=True))


if __name__ == '__main__':
    main()
