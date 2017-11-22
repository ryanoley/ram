import os
import json
import pandas as pd

from ram import config
from ram.strategy.statarb.statarb_config import implementation_top_models
from ram.data.data_constructor_blueprint import DataConstructorBlueprint

from ram.strategy.base import read_json, write_json, clean_top_params


def main():

    output_dir = check_implementation_folder_structure()

    top_params = clean_top_params(implementation_top_models)

    for i, (param, run) in enumerate(zip(*top_params)):
        # Read meta
        run_meta = get_run_meta(run)
        data_version = run_meta['prepped_data_version']
        # Locate seccodes
        seccodes = get_univ_seccodes(data_version)
        # Create blueprint for each unique data version
        data_meta = get_prepped_data_meta(data_version)

        # Create and write
        blueprint = DataConstructorBlueprint(strategy_name='StatArbStrategy',
                                             constructor_type='seccodes',
                                             description=run)

        blueprint.features = data_meta['blueprint']['features']
        blueprint.seccodes_filter_arguments['seccodes'] = seccodes
        blueprint.seccodes_filter_arguments['start_date'] = 'NONE'
        blueprint.seccodes_filter_arguments['end_date'] = 'NONE'

        blueprint_path = os.path.join(
            output_dir, 'current_blueprint_{}.json'.format(data_version))

        write_json(blueprint.to_json(), blueprint_path)


def check_implementation_folder_structure(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    if not os.path.isdir(implementation_dir):
        os.mkdir(implementation_dir)
    statarb_path = os.path.join(implementation_dir, 'StatArbStrategy')
    if not os.path.isdir(statarb_path):
        os.mkdir(statarb_path)
    path = os.path.join(statarb_path, 'daily_raw_data')
    if not os.path.isdir(path):
        os.mkdir(path)
    path = os.path.join(statarb_path, 'preprocessed_data')
    if not os.path.isdir(path):
        os.mkdir(path)
    # Each time this is run it creates a new version
    all_files = os.listdir(path)
    if all_files:
        all_files.sort()
        new_ind = int(all_files[-1].split('_')[-1]) + 1
    else:
        new_ind = 1
    new_path = os.path.join(path, 'preprocess_{:03d}'.format(new_ind))
    os.mkdir(new_path)
    return new_path


def get_univ_seccodes(version, prepped_data_dir=config.PREPPED_DATA_DIR):
    # Get most recent seccodes from data file. This will be the live
    # universe
    path = os.path.join(prepped_data_dir, 'StatArbStrategy', version)
    data_files = [x for x in os.listdir(path) if x.find('_data.csv') > -1]
    if 'market_index_data.csv' in data_files:
        data_files.remove('market_index_data.csv')
    data_files.sort()
    last_data_file_name = data_files[-1]
    data = pd.read_csv(os.path.join(path, last_data_file_name))
    return data.SecCode.unique().tolist()


def get_prepped_data_meta(version, prepped_data_dir=config.PREPPED_DATA_DIR):
    path = os.path.join(prepped_data_dir, 'StatArbStrategy',
                        version, 'meta.json')
    return read_json(path)


def get_run_meta(run_name, simulation_dir=config.SIMULATIONS_DATA_DIR):
    path = os.path.join(simulation_dir, 'StatArbStrategy',
                        run_name, 'meta.json')
    return read_json(path)


if __name__ == '__main__':
    main()
