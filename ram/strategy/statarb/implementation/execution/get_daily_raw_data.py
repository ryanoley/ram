import os
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config
from ram.strategy.base import read_json

from ram.data.data_handler_sql import DataHandlerSQL
from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint


def main():

    # RAW DAILY DATA
    pull_daily_raw_data()

    # MAPPING
    unique_seccodes = write_ticker_mapping()

    # PREP FOR BLOOMBERG DATA
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


def pull_daily_raw_data():
    # Location of production blueprints
    blueprints = get_unique_blueprints()
    dc = DataConstructor()
    for b, blueprint in tqdm(blueprints.iteritems()):
        # Convert from universe to universe_live
        blueprint.constructor_type = 'universe_live'
        # Name
        today = dt.datetime.utcnow()
        blueprint.output_file_name = \
            '{}_{}'.format(today.strftime('%Y%m%d'), b.strip('.json'))
        # Run
        dc.run_live(blueprint, 'StatArbStrategy')
    return


def write_ticker_mapping():
    unique_seccodes = get_unique_seccodes_from_data()
    dh = DataHandlerSQL()
    mapping = dh.get_ticker_seccode_map()
    mapping = mapping.loc[mapping.SecCode.isin(unique_seccodes)]
    # Hard-coded SecCodes for indexes
    indexes = pd.DataFrame()
    indexes['SecCode'] = ['50311', '11113']
    indexes['Ticker'] = ['$SPX.X', '$VIX.X']
    indexes['Issuer'] = ['SPX', 'VIX']

    mapping = mapping.append(indexes).reset_index(drop=True)

    # Get hash table for odd tickers
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live_pricing',
                        'odd_ticker_hash.json')
    odd_tickers = json.load(open(path, 'r'))
    mapping.Ticker = mapping.Ticker.replace(to_replace=odd_tickers)

    # Write ticker mapping to file
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live_pricing',
                        'ticker_mapping.csv')
    mapping.to_csv(path, index=None)

    return unique_seccodes


def get_unique_blueprints():
    path = os.path.join(
        config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy',
        'trained_models', statarb_config.trained_models_dir_name,
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


def get_unique_seccodes_from_data():
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    all_files.remove('market_index_data.csv')
    max_date_prefix = max([x.split('_')[0] for x in all_files])
    # Read in dates for files
    todays_files = [x for x in all_files if x.find(max_date_prefix) > -1]
    seccodes = np.array([])
    for f in todays_files:
        data = pd.read_csv(os.path.join(raw_data_dir, f))
        # Filter only active securities
        data = data[data.Date == data.Date.max()]
        seccodes = np.append(seccodes, data.SecCode.astype(str).unique())
    return np.unique(seccodes)


if __name__ == '__main__':
    main()
