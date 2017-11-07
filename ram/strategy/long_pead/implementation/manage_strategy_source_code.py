import os
import shutil
import subprocess

from google.cloud import storage

from ram import config
from ram.utils.packages import find_installed_ram


def _make_setup_file(new_files, path):
    PACKAGES = ['ram', 'ram/analysis', 'ram/aws', 'ram/data',
                'ram/strategy', 'ram/utils'] + new_files
    outfile = open(path, 'w')
    outfile.write("import os\n")
    outfile.write("from distutils.core import setup\n\n")
    outfile.write("DISTNAME = 'ram'\n")
    outfile.write("PACKAGES = [\n")
    for p in PACKAGES:
        outfile.write('\t' + p + ',\n')
    outfile.write("]\n")
    outfile.write("setup(version='0.1.0', name=DISTNAME, packages=PACKAGES)")
    outfile.close()


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
        if args.cloud:
            if not os.path.isdir(dest):
                os.mkdir(dest)
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
                run_file_name = r.replace(run_path + '/', '')
                # See if directory exists
                if len(run_file_name.split('/')) > 1:
                    dirs = []
                    for p in run_file_name.split('/')[:-1]:
                        dirs.append(p)
                        temp_path = os.path.join(dest, *dirs)
                        if not os.path.isdir(temp_path):
                            os.mkdir(temp_path)
                # Create new file path and download
                new_path = os.path.join(dest, run_file_name)
                blob.download_to_filename(new_path)

        else:
            src = os.path.join(os.getenv('DATA'), 'ram', 'simulations',
                               'LongPeadStrategy', run_name,
                               'strategy_source_copy')

            shutil.copytree(src, dest)
        print('[Manager] - Copy Complete')
        # Extract new directories
        new_dirs = [x[0] for x in os.walk(dest)]
        new_dirs = [x for x in new_dirs if x.find('__pycache__') < 0]
        new_dirs = [x.replace(dest.replace('/long_pead', '') + '/', '')
            for x in new_dirs]
        new_dirs = ['ram/strategy/' + d for d in new_dirs]
        # Setup
        setup_file_path = os.path.join(dest, 'setup.py')
        _make_setup_file(new_dirs, setup_file_path)
        subprocess.call(['python', setup_file_path, 'install'])
        print('[Manager] - SETUP Complete')

    elif args.delete_strategy_source_code:
        path = os.path.join(find_installed_ram(), 'strategy', 'long_pead')
        if os.path.isdir(path):
            print("[Manager] - Deleting: " + path)
            shutil.rmtree(path)
