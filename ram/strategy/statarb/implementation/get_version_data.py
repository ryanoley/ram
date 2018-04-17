"""
This script is to be run every morning. The shell script should live in
ram/tasks.
"""
import os
import json
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint


def main():
    pull_version_data()


def pull_version_data():
    blueprints = get_unique_blueprints()

    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'archive',
                        'version_data')

    dc = DataConstructor()

    print('[[ Pulling data ]]')

    prefix = dt.date.today().strftime('%Y%m%d')

    for b, blueprint in blueprints.iteritems():
        # Manually set instance attributes for running live
        blueprint.constructor_type = 'universe_live'
        blueprint.output_file_dir = path
        blueprint.output_file_name = '{}_{}'.format(prefix, b)
        # Pull data via DataConstructor
        dc.run_live(blueprint)
        print('  {} completed'.format(b))

    # Rename market_index_data with prefix
    old_path = os.path.join(path, 'market_index_data.csv')
    new_path = os.path.join(path, '{}_market_index_data.csv'.format(prefix))
    if os.path.isfile(new_path):
        os.remove(new_path)
    os.rename(old_path, new_path)

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


if __name__ == '__main__':
    main()
