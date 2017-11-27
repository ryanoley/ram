import os
import json
from tqdm import tqdm
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config
from ram.strategy.base import read_json

from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint


def main():
    base_path = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy')
    raw_data_path = os.path.join(base_path, 'daily_raw_data')
    blueprints_path = os.path.join(base_path, 'preprocessed_data',
                                   statarb_config.preprocessed_data_dir)
    blueprints = os.listdir(blueprints_path)
    blueprints = [x for x in blueprints if x.find('blueprint') > -1]
    dc = DataConstructor()
    # Iterate
    for b in tqdm(blueprints):
        bp = read_json(os.path.join(blueprints_path, b))
        blueprint = DataConstructorBlueprint(blueprint_json=bp)
        # Set date parameters
        today = dt.datetime.utcnow()
        start_date = (today - dt.timedelta(days=380)).strftime('%Y-%m-%d')
        end_date = (today - dt.timedelta(days=1)).strftime('%Y-%m-%d')
        blueprint.seccodes_filter_arguments['start_date'] = start_date
        blueprint.seccodes_filter_arguments['end_date'] = end_date
        # Output name is just the date
        blueprint.seccodes_filter_arguments['output_file_name'] = \
            '{}_{}'.format(today.strftime('%Y%m%d'), b.strip('.json'))
        # Run in the morning
        dc.run_live(blueprint, 'StatArbStrategy')


if __name__=='__main__':
    main()
