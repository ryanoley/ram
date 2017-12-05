import os
import json
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
    dh = DataHandlerSQL()
    mapping = dh.get_ticker_seccode_map()
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live_pricing',
                        'ticker_mapping.csv')
    mapping.to_csv(path, index=None)


if __name__ == '__main__':
    main()
