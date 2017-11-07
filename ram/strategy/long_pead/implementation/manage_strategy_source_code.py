import os
import stat
import shutil
from distutils.sysconfig import get_python_lib


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
            version = args.restart_run
        else:
            version = runs.Run.iloc[int(args.copy_run)]

        if args.cloud:
            pass
        else:
            src = os.path.join(os.getenv('DATA'), 'ram', 'simulations',
                               'LongPeadStrategy', version,
                               'strategy_source_copy')
            dest = os.path.join(os.getenv('GITHUB'), 'ram', 'ram',
                                'strategy', 'long_pead')
            shutil.copytree(src, dest)

    elif args.delete_strategy_source_code:
        path = os.path.join(get_python_lib(), 'ram', 'strategy', 'long_pead')
        if os.path.isdir(path):
            print("Deleting: ", path)
            shutil.rmtree(path)
