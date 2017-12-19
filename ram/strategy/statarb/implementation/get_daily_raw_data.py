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

    # Location of production blueprints
    blueprints_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                   'StatArbStrategy',
                                   'preprocessed_data',
                                   statarb_config.preprocessed_data_dir)
    blueprints = os.listdir(blueprints_path)
    blueprints = [x for x in blueprints if x.find('blueprint') > -1]

    dc = DataConstructor()

    for b in tqdm(blueprints):
        blueprint = DataConstructorBlueprint(
            blueprint_json=read_json(os.path.join(blueprints_path, b)))
        # Convert from universe to universe_live
        blueprint.constructor_type = 'universe_live'
        # Name
        today = dt.datetime.utcnow()
        blueprint.output_file_name = \
            '{}_{}'.format(today.strftime('%Y%m%d'), b.strip('.json'))
        # Run
        dc.run_live(blueprint, 'StatArbStrategy')

    # Get ticker mapping
    unique_seccodes = get_unique_seccodes_from_data()

    dh = DataHandlerSQL()
    mapping = dh.get_ticker_seccode_map()
    mapping = mapping.loc[mapping.SecCode.isin(unique_seccodes)]

    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live_pricing',
                        'ticker_mapping.csv')
    mapping.to_csv(path, index=None)

    # Get dividendfactor, splitfactor data
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
