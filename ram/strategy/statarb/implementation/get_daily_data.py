"""
This script is to be run every morning. The shell script should live in
ram/tasks.
"""
import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from shutil import copyfile

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint


def main():
    check_implementation_folder_structure()
    clear_live_dir()
    pull_version_data()
    copy_version_data_to_archive()
    unique_seccodes = get_unique_seccodes_from_data()
    write_seccode_ticker_mapping(unique_seccodes)
    write_scaling_data(unique_seccodes)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def check_implementation_folder_structure():
    """
    Folder structure is created according to how the system expects
    it to function.
    """
    ddir = config.IMPLEMENTATION_DATA_DIR

    path = os.path.join(ddir, 'StatArbStrategy')
    _check_create(path)

    path2 = os.path.join(path, 'live')
    _check_create(path2)

    # Archive has many different file types associated with it
    path2 = os.path.join(path, 'archive')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'version_data')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'ticker_mapping')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'qad_scaling')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'bloomberg_scaling')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'live_pricing')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'size_containers')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'allocations')
    _check_create(path2)

    return


def _check_create(path):
    if not os.path.isdir(path):
        os.mkdir(path)
    return


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clear_live_dir():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'live')
    all_files = os.listdir(path)
    for f in all_files:
        os.remove(os.path.join(path, f))
    return


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def pull_version_data():

    blueprints = get_unique_blueprints()

    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'live')

    dc = DataConstructor()

    print('[[ Pulling data ]]')
    for b, blueprint in blueprints.iteritems():
        # Manually set instance attributes for running live
        blueprint.constructor_type = 'universe_live'
        blueprint.output_file_dir = path
        blueprint.output_file_name = b

        dc.run_live(blueprint)
        print('  {} completed'.format(b))
    return


def get_unique_blueprints():
    """
    These blueprints come from JSON files that were prepared by the
    implementation training script, in the `trained_models` directory
    """
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'trained_models',
                        statarb_config.trained_models_dir_name,
                        'run_map.json')
    run_map = json.load(open(path, 'r'))
    # Extract unique prepped data version Blueprints
    blueprints = {}
    for param in run_map:
        version = param['prepped_data_version']
        if version in blueprints:
            continue
        blueprints[version] = DataConstructorBlueprint(
            blueprint_json=param['blueprint'])
    return blueprints


def copy_version_data_to_archive():

    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'live')
    # Get version and market data, and copy
    all_files = os.listdir(path)
    version_files = [x for x in all_files if x.find('version') > -1]

    # Copy version data
    path2 = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'archive', 'version_data')
    prefix = dt.date.today().strftime('%Y%m%d')
    for v in version_files:
        copyfile(os.path.join(path, v),
                 os.path.join(path2, '{}_{}'.format(prefix, v)))

    # Market Data
    copyfile(os.path.join(path, 'market_index_data.csv'),
             os.path.join(path2, '{}_market_index_data.csv'.format(prefix)))

    return


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_unique_seccodes_from_data():
    """
    For all versions of data, get last two file's worth of unique
    seccodes. Older file is for overlapping time periods
    """
    print('[[ Getting unique SecCodes ]]')
    # Get prepped data versions
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'trained_models',
                        statarb_config.trained_models_dir_name,
                        'run_map.json')
    run_map = json.load(open(path, 'r'))
    versions = list(set([x['prepped_data_version'] for x in run_map]))
    seccodes = []
    for v in versions:
        # Get all data files, and exclude market data
        path = os.path.join(config.PREPPED_DATA_DIR, 'StatArbStrategy', v)
        files = [x for x in os.listdir(path) if x.find('_data.csv') > -1]
        if 'market_index_data.csv' in files:
            files.remove('market_index_data.csv')
        files.sort()
        # Look at IDs for only the last two files
        for f in files[-2:]:
            data = pd.read_csv(os.path.join(path, f))
            seccodes += data.SecCode.astype(str).unique().tolist()
    return list(set(seccodes))


def write_seccode_ticker_mapping(unique_seccodes):
    """
    Maps Tickers to SecCodes, and writes in archive and in live_pricing
    directory.
    """
    print('[[ Mapping Tickers to SecCodes ]]')

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

    # Re-order for live pricing script
    mapping = mapping[['Cusip', 'Issuer', 'SecCode', 'Ticker']]

    # Get hash table for odd tickers
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'odd_ticker_hash.json')
    odd_tickers = json.load(open(path, 'r'))
    mapping.Ticker = mapping.Ticker.replace(to_replace=odd_tickers)

    # Re-org columns
    mapping = mapping[['SecCode', 'Ticker', 'Issuer']]

    # Write ticker mapping to two locations
    today = dt.datetime.utcnow()
    file_name = '{}_{}'.format(today.strftime('%Y%m%d'), 'ticker_mapping.csv')
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'archive',
                        'ticker_mapping',
                        file_name)
    mapping.to_csv(path, index=None)

    # For live_prices pull
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live',
                        'ticker_mapping.csv')
    mapping.to_csv(path, index=None)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def write_scaling_data(unique_seccodes):
    # Scaling
    print('[[ Getting Scaling Data ]]')
    dh = DataHandlerSQL()
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=7)
    scaling = dh.get_seccode_data(seccodes=unique_seccodes,
                                  features=['DividendFactor', 'SplitFactor'],
                                  start_date=start_date,
                                  end_date=end_date)
    scaling = scaling[scaling.Date == scaling.Date.max()]

    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live',
                        'seccode_scaling.csv')
    scaling.to_csv(path, index=None)

    today = dt.datetime.utcnow()
    file_name = '{}_{}'.format(today.strftime('%Y%m%d'), 'seccode_scaling.csv')
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'archive',
                        'qad_scaling',
                        file_name)

    scaling.to_csv(path, index=None)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__ == '__main__':
    main()
