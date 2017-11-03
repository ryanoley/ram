import os
import json
import pandas as pd

from google.cloud import storage

from ram import config

client = storage.Client()
bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)


def update_prepped_data_gcp(strategy, version):
    # GCP Storage files for  Strategy/Version
    gs_files = [x.name for x in bucket.list_blobs()]
    gs_files = [x for x in gs_files if x.find('prepped_data') > -1]
    gs_files = [x for x in gs_files if x.split('/')[1] == strategy]
    gs_files = [x for x in gs_files if x.split('/')[2] == version]
    gs_files = [x.split('/')[-1] for x in gs_files]
    gs_version_path = 'prepped_data/{}/{}/'.format(strategy, version)

    # Local Files
    version_path = os.path.join(config.PREPPED_DATA_DIR, strategy, version)
    local_files = os.listdir(version_path)

    # Diff to see which files to upload
    upload_files = [x for x in local_files if x not in gs_files]

    # See if the local version has updated existing files
    meta = json.load(open(os.path.join(version_path, 'meta.json'), 'r'))

    if 'newly_created_files' in meta:
        upload_files.extend(meta['newly_created_files'])

    # If market data exists, this was automatically updated by dp
    if ('market_index_data.csv' in local_files) and \
            ('market_index_data.csv' not in upload_files) and \
            (len(upload_files) > 0):
        upload_files.extend('market_index_data.csv')

    if upload_files:
        print('Confirm upload of the following files: ')
        print(upload_files)
        user_input = raw_input('Submit y: ')
        if user_input != 'y':
            return
        for f in upload_files:
            # Upload
            blob = bucket.blob(os.path.join(gs_version_path, f))
            blob.upload_from_filename(os.path.join(version_path, f))
            print('Uploaded {}/{}/{}'.format(strategy, version, f))
        meta['newly_created_files'] = []
        json.dump(meta, open(os.path.join(version_path, 'meta.json'), 'w'))

    else:
        print('\nNo new files to upload for {}/{}\n'.format(strategy, version))


if __name__ == '__main__':

    import argparse

    from ram.data.data_constructor import print_strategies
    from ram.data.data_constructor import print_strategy_versions
    from ram.data.data_constructor import get_version_name
    from ram.data.data_constructor import get_strategy_name

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-ls', '--list_strategies', action='store_true',
        help='List all available strategies')
    parser.add_argument(
        '-s', '--strategy', type=str,
        help='Strategy to manipulate')

    parser.add_argument(
        '-lv', '--list_versions', action='store_true',
        help='List all versions of prepped data for a strategy')
    parser.add_argument(
        '-v', '--version', type=str,
        help='Version to be uploaded')

    parser.add_argument(
        '-up', '--upload', action='store_true',
        help='This flag must be included to upload data to GCP')

    args = parser.parse_args()

    if args.list_strategies:
        print print_strategies()
    elif args.list_versions:
        if args.strategy is None:
            print '\n[ERROR] - Strategy must be included via `-s`\n'
        else:
            print_strategy_versions(get_strategy_name(args.strategy))
    elif args.strategy and args.version and args.upload:
        strategy = get_strategy_name(args.strategy)
        version = get_version_name(strategy, args.version)
        update_prepped_data_gcp(strategy=strategy, version=version)