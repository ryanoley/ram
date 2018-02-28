import os
import json
import pandas as pd

from google.cloud import storage

from ram import config
from ram.data.data_constructor import _print_line_underscore


def update_prepped_data_gcp(strategy, version):

    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)

    # Local Files
    version_path = os.path.join(config.PREPPED_DATA_DIR, strategy, version)
    local_files = os.listdir(version_path)

    # GCP Storage files for  Strategy/Version
    gs_files = [x.name for x in bucket.list_blobs()]
    gs_files = [x for x in gs_files if x.find('prepped_data') > -1]
    gs_files = [x for x in gs_files if x.split('/')[1] == strategy]
    gs_files = [x for x in gs_files if x.split('/')[2] == version]
    gs_files = [x.split('/')[-1] for x in gs_files]
    gs_version_path = 'prepped_data/{}/{}/'.format(strategy, version)

    # Which files don't exist in cloud storage
    upload_files = [x for x in local_files if x not in gs_files]

    # See if the local version has updated existing files
    meta = json.load(open(os.path.join(version_path, 'meta.json'), 'r'))
    if 'newly_created_files' in meta:
        upload_files.extend(meta['newly_created_files'])
        meta['newly_created_files'] = []
        json.dump(meta, open(os.path.join(version_path, 'meta.json'), 'w'))

    # If market data exists, this should always be updated
    if ('market_index_data.csv' in local_files) and \
            ('market_index_data.csv' not in upload_files) and \
            (len(upload_files) > 0):
        upload_files.extend(['market_index_data.csv'])

    upload_files = list(set(upload_files))
    upload_files.sort()
    if upload_files:
        # If uploading new files, upload meta
        upload_files.append('meta.json')
        if not confirm(upload_files):
            return
        for f in upload_files:
            # Upload
            blob = bucket.blob(os.path.join(gs_version_path, f))
            blob.upload_from_filename(os.path.join(version_path, f))
            print('Uploaded: {}/{}/{}'.format(strategy, version, f))

    else:
        print('\nNo new files to upload for {}/{}\n'.format(strategy, version))


def print_trained_model_dirs(strategy):
    gs_model_dirs = get_trained_model_dirs(strategy)
    headline = 'Available Trained Model Directories for {}'.format(strategy)
    _print_line_underscore(headline)
    for i, model in enumerate(gs_model_dirs):
        print(" [{}]\t{}".format(i, model))
    print('\n')


def get_trained_model_dirs(strategy):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    # GCP Directories for implementation
    gs_files = [x.name for x in bucket.list_blobs()]
    gs_files = [x for x in gs_files if x.find('implementation') > -1]
    gs_files = [x for x in gs_files if x.find(strategy) > -1]
    gs_files = [x for x in gs_files if x.find('trained_models') > -1]
    gs_model_dirs = list(set([x.split('/')[3] for x in gs_files]))
    gs_model_dirs.sort()
    return gs_model_dirs


def get_trained_model_name(strategy, name_num):
    model_dirs = get_trained_model_dirs(strategy)
    if name_num in model_dirs:
        return name_num
    else:
        return model_dirs[int(name_num)]


def download_trained_models_gcp(strategy, model_dir_name):
    gcp_path = "gs://ram_data/implementation/{}/trained_models/{}".format(
        strategy, model_dir_name)
    local_path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                              strategy, 'trained_models')

    copy_string = "gsutil -m cp -r {} {}".format(gcp_path, local_path)
    os.system(copy_string)


def confirm(upload_files):

    print('\nConfirm upload of the following files:')
    temp = upload_files + ['', '']
    for i in range(len(temp) / 3):
        print('{}\t{}\t{}'.format(*temp[(i*3):((i+1)*3)]))
    print('\n')
    user_input = raw_input('Continue? Type `1234`: ')
    if user_input != '1234':
        return False
    else:
        return True


if __name__ == '__main__':

    import argparse

    from ram.data.data_constructor import print_strategies
    from ram.data.data_constructor import print_data_versions
    from ram.data.data_constructor import get_strategy_name
    from ram.data.data_constructor import get_data_version_name

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-ls', '--list_strategies', action='store_true',
        help='List all available strategies')
    parser.add_argument(
        '-s', '--strategy_name', type=str,
        help='Strategy to manipulate')

    parser.add_argument(
        '-ld', '--list_data_versions', action='store_true',
        help='List all versions of prepped data for a strategy')
    parser.add_argument(
        '-d', '--data_version', type=str,
        help='Version to be uploaded')

    parser.add_argument(
        '-lm', '--list_trained_models', action='store_true',
        help='List all directories on GCP platform in '
        'implementation/trained_models')
    parser.add_argument(
        '-m', '--trained_models_dir', type=str,
        help='Directory of models to be downloaded')

    parser.add_argument(
        '--upload', action='store_true',
        help='This flag must be included to upload data to GCP')

    parser.add_argument(
        '--download', action='store_true',
        help='This flag must be included to download data from GCP')

    args = parser.parse_args()

    if args.list_strategies:
        print print_strategies()

    elif args.list_data_versions:
        if args.strategy_name is None:
            print '\n[ERROR] - Name or index must be included via `-s`\n'
        else:
            strategy = get_strategy_name(args.strategy_name)
            print_data_versions(strategy)

    elif args.list_trained_models:
        if args.strategy_name is None:
            print '\n[ERROR] - Name or index must be included via `-s`\n'
        else:
            strategy = get_strategy_name(args.strategy_name)
            print_trained_model_dirs(strategy)

    elif args.strategy_name and args.data_version and args.upload:
        strategy = get_strategy_name(args.strategy_name)
        version = get_data_version_name(strategy, args.data_version)
        update_prepped_data_gcp(strategy=strategy, version=version)

    elif args.strategy_name and args.trained_models_dir and args.download:
        strategy = get_strategy_name(args.strategy_name)
        model_dir = get_trained_model_name(strategy, args.trained_models_dir)
        download_trained_models_gcp(strategy, model_dir)
