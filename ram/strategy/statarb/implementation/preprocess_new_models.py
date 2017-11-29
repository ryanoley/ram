import os
import json
import pandas as pd

from ram import config
from ram.strategy.statarb.statarb_config import implementation_top_models
from ram.data.data_constructor_blueprint import DataConstructorBlueprint

from ram.strategy.base import read_json, write_json


def main():

    output_dir = check_implementation_folder_structure()

    top_params = import_current_top_params()

    data_versions = []
    for key, params in top_params.iteritems():
        # Remove column index from run name
        run_name = key[:key.rfind('_')]
        # Get data version
        data_version = params['prepped_data_version']
        if data_version in data_versions:
            continue
        data_versions.append(data_version)
        # Create blueprint for each unique data version
        data_meta = get_prepped_data_meta(data_version)
        blueprint = data_meta['blueprint']
        blueprint_path = os.path.join(
            output_dir, 'current_blueprint_{}.json'.format(data_version))
        write_json(blueprint, blueprint_path)

    preprocess_name = os.path.basename(output_dir)
    print('\nUpdate `statarb_config/preprocessed_data_dir` with preprocess '
          'version: {}\n'.format(preprocess_name))


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
    path = os.path.join(statarb_path, 'trained_models')
    if not os.path.isdir(path):
        os.mkdir(path)
    pp_path = os.path.join(statarb_path, 'preprocessed_data')
    if not os.path.isdir(pp_path):
        os.mkdir(pp_path)
    # Each time this is run it creates a new version
    all_files = os.listdir(pp_path)
    if all_files:
        all_files.sort()
        new_ind = int(all_files[-1].split('_')[-1]) + 1
    else:
        new_ind = 1
    new_path = os.path.join(pp_path, 'preprocess_{:04d}'.format(new_ind))
    os.mkdir(new_path)
    return new_path


def import_current_top_params(
        top_params_dir=os.path.dirname(os.path.abspath(__file__))):
    return read_json(os.path.join(top_params_dir, 'current_top_params.json'))


def get_prepped_data_meta(version, prepped_data_dir=config.PREPPED_DATA_DIR):
    path = os.path.join(prepped_data_dir, 'StatArbStrategy',
                        version, 'meta.json')
    return read_json(path)


if __name__ == '__main__':
    main()
