"""
This script is to be run every morning. The shell script should live in
ram/tasks.
"""
import os
import json
import numpy as np
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint


def main():
    check_implementation_folder_structure()
    pull_daily_data()
    unique_seccodes = get_unique_seccodes_from_data()
    write_seccode_ticker_mapping(unique_seccodes)
    write_scaling_data(unique_seccodes)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def pull_daily_data():
    blueprints = get_unique_blueprints()
    dc = DataConstructor()
    print('[[ Pulling data ]]')
    for b, blueprint in blueprints.iteritems():
        # Convert from universe to universe_live
        blueprint.constructor_type = 'universe_live'
        # Name
        today = dt.datetime.utcnow()
        blueprint.output_file_name = \
            '{}_{}'.format(today.strftime('%Y%m%d'), b.strip('.json'))
        # Run
        dc.run_live(blueprint, 'StatArbStrategy')
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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_unique_seccodes_from_data():
    print('[[ Getting unique SecCodes ]]')
    # Get all version data files for TODAY
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy', 'daily_data')
    all_files = os.listdir(data_dir)
    version_files = [x for x in all_files if x.find('version') > -1]
    max_date_prefix = max([x.split('_')[0] for x in version_files])
    # Read in dates for files
    todays_files = [x for x in version_files if x.find(max_date_prefix) > -1]
    seccodes = np.array([])
    for f in todays_files:
        data = pd.read_csv(os.path.join(data_dir, f))
        # Filter only active securities
        data = data[data.Date == data.Date.max()]
        seccodes = np.append(seccodes, data.SecCode.astype(str).unique())
    return np.unique(seccodes)


def write_seccode_ticker_mapping(unique_seccodes):
    """
    Maps Tickers to SecCodes, and writes in archive and in live_pricing
    directory.
    """
    print('[[ Mapping Tickers to SecCodes ]]')
    dh = DataHandlerSQL()
    mapping = dh.get_ticker_seccode_map()
    mapping = mapping.loc[mapping.SecCode.isin(unique_seccodes)]
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
                        'live_pricing',
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
                        'daily_data',
                        file_name)
    mapping.to_csv(path, index=None)

    # For live_prices pull
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live_pricing',
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
                        'live_pricing',
                        'seccode_scaling.csv')
    scaling.to_csv(path, index=None)

    today = dt.datetime.utcnow()
    file_name = '{}_{}'.format(today.strftime('%Y%m%d'), 'seccode_scaling.csv')
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'daily_data',
                        file_name)

    scaling.to_csv(path, index=None)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def check_implementation_folder_structure(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    if not os.path.isdir(implementation_dir):
        os.mkdir(implementation_dir)
    statarb_path = os.path.join(implementation_dir, 'StatArbStrategy')
    if not os.path.isdir(statarb_path):
        os.mkdir(statarb_path)
    path = os.path.join(statarb_path, 'daily_data')
    if not os.path.isdir(path):
        os.mkdir(path)
    return


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__ == '__main__':
    main()
