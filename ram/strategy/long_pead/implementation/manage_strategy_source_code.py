import os
import shutil

from google.cloud import storage

from ram import config
from ram.utils.packages import find_installed_ram


if __name__ == '__main__':

    import argparse

    from ram.analysis.run_manager import get_run_data

    parser = argparse.ArgumentParser()

    # Cloud tag must be included anytime working in GCP
    parser.add_argument(
        '-c', '--cloud', action='store_true',
        help='Tag must be added for GCP implementation')

    parser.add_argument(
        '-lr', '--list_runs', action='store_true',
        help='List all simulations for a strategy')

    parser.add_argument(
        '-cr', '--copy_run', type=int, default=None,
        help='If something craps out, use this tag. Send in run name'
    )

    parser.add_argument(
        '-ds', '--delete_strategy_source_code', action='store_true',
        help='Deletes local environment version of strategy'
    )

    args = parser.parse_args()

    if args.list_runs:
        runs = get_run_data('LongPeadStrategy', args.cloud)
        # Adjust column width
        runs['Description'] = runs.Description.apply(lambda x: x[:20] + ' ...')
        print(runs)

    elif args.copy_run:
        runs = get_run_data('LongPeadStrategy', args.cloud)
        if args.copy_run in runs.Run.values:
            run_name = args.restart_run
        else:
            run_name = runs.Run.iloc[int(args.copy_run)]
        # Local destination of source data
        dest = os.path.join(os.getenv('GITHUB'), 'ram', 'ram',
                            'strategy', 'long_pead')
        import pdb; pdb.set_trace()
        if args.cloud:
            # Folder structure - This is a sucky implementation
            if not os.path.isdir(dest):
                os.mkdir(dest)
            if not os.path.isdir(os.path.join(dest, 'constructor')):
                os.mkdir(os.path.join(dest, 'constructor'))
            if not os.path.isdir(os.path.join(dest, 'data')):
                os.mkdir(os.path.join(dest, 'data'))
            if not os.path.isdir(os.path.join(dest, 'signals')):
                os.mkdir(os.path.join(dest, 'signals'))
            # Get files from Storage
            client = storage.Client()
            bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
            all_files = [x.name for x in bucket.list_blobs()]
            run_path = os.path.join('simulations', 'LongPeadStrategy',
                                    run_name, 'strategy_source_copy')
            run_files = [x for x in all_files if x.find(run_path) > -1]
            run_files = [x for x in run_files if x.find('.pyc') == -1]
            for r in run_files:
                blob = bucket.blob(r)
                run_file_name = r.replace(run_path, '')
                new_path = os.path.join(dest, run_file_name)
                blob.download_to_filename(new_path)

        else:
            src = os.path.join(os.getenv('DATA'), 'ram', 'simulations',
                               'LongPeadStrategy', run_name,
                               'strategy_source_copy')

            shutil.copytree(src, dest)

    elif args.delete_strategy_source_code:
        path = os.path.join(find_installed_ram(), 'ram',
                            'strategy', 'long_pead')
        if os.path.isdir(path):
            print("Deleting: ", path)
            shutil.rmtree(path)
