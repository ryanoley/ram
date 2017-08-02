import os
import sys
import json
import shutil
import inspect
import pandas as pd
from tqdm import tqdm
import datetime as dt

from StringIO import StringIO
from google.cloud import storage

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config

from gearbox import convert_date_array

from ram.data.data_constructor import DataConstructor
from ram.utils.documentation import get_git_branch_commit
from ram.utils.documentation import prompt_for_description


class Strategy(object):

    __metaclass__ = ABCMeta

    def __init__(self,
                 prepped_data_version='NODATA',
                 write_flag=False,
                 prepped_data_dir=config.PREPPED_DATA_DIR,
                 simulation_output_dir=config.SIMULATION_OUTPUT_DIR,
                 gcp_implementation=False):
        """
        Parameters
        ----------
        prepped_data_version : str
            This is the name of the prepped data version, e.g.: version_0002
        write_flag : bool
            Whether to create an output directory and write results to file
        prepped_data_dir : str
            Location of the global prepped data directory, not specific to the
            Strategy or version provided. Defaults to what is in the global
            config file
        simulation_output_dir : str
            Location of where written output will go. Defaults to what is in
            the global config file
        """
        self._write_flag = write_flag
        self._data_version = prepped_data_version
        self._gcp_implementation = gcp_implementation
        self._max_run_time_index = -1  # This is for restart functionality
        if self._gcp_implementation:
            self._gcp_client = storage.Client()
            self._bucket = self._gcp_client.get_bucket(
                config.GCP_STORAGE_BUCKET_NAME)
            self._prepped_data_dir = os.path.join('prepped_data',
                                                  self.__class__.__name__,
                                                  prepped_data_version)
            # CURRENTLY WRITING TO LOCAL MACHINE
            self._strategy_output_dir = os.path.join(simulation_output_dir,
                                                     self.__class__.__name__)
        else:
            self._prepped_data_dir = os.path.join(prepped_data_dir,
                                                  self.__class__.__name__,
                                                  prepped_data_version)
            self._strategy_output_dir = os.path.join(simulation_output_dir,
                                                     self.__class__.__name__)

    # ~~~~~~ RUN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def start(self, description=None):
        self._print_prepped_data_meta()
        self._get_prepped_data_file_names()
        self._create_run_output_dir()
        self._copy_source_code()
        self._create_meta_file(description)
        self._write_column_parameters_file()
        for i in tqdm(range(len(self._prepped_data_files))):
            self.run_index(i)
        self._shutdown_simulation()

    def restart(self, run_name):
        self._import_run_meta_for_restart(run_name)
        self._print_prepped_data_meta()
        self._get_prepped_data_file_names()
        self._get_max_run_time_index_for_restart()
        for i in tqdm(range(len(self._prepped_data_files))):
            self.run_index(i)
        self._shutdown_simulation()

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _create_run_output_dir(self):
        if self._write_flag:
            # Create Strategy folder if it doesn't exist
            if not os.path.exists(self._strategy_output_dir):
                os.makedirs(self._strategy_output_dir)
            # Get all run versions for increment for this run
            all_dirs = [x for x in os.listdir(
                self._strategy_output_dir) if x[:3] == 'run']
            if all_dirs:
                new_ind = max([int(x.split('_')[1]) for x in all_dirs]) + 1
            else:
                new_ind = 1
            self.run_dir = os.path.join(self._strategy_output_dir,
                                        'run_{0:04d}'.format(new_ind))
            os.mkdir(self.run_dir)
            # Output directory setup
            self.strategy_output_dir = os.path.join(
                self.run_dir, 'index_outputs')
            os.makedirs(self.strategy_output_dir)

    def _copy_source_code(self):
        if self._write_flag:
            # Copy source code for Strategy
            source_path = os.path.dirname(inspect.getabsfile(self.__class__))
            dest_path = os.path.join(self.run_dir, 'strategy_source_copy')
            copytree(source_path, dest_path)

    def _create_meta_file(self, user_description=None):
        if self._write_flag:
            # To aid unittest
            desc = user_description if user_description else \
                prompt_for_description()
            git_branch, git_commit = get_git_branch_commit()
            # Create meta object
            run_meta = {
                'prepped_data_version': self._data_version,
                'latest_git_commit': git_commit,
                'git_branch': git_branch,
                'description': desc,
                'completed': False,
                'start_time': str(dt.datetime.utcnow())[:19]
            }
            with open(os.path.join(self.run_dir, 'meta.json'), 'w') as outfile:
                json.dump(run_meta, outfile)
            outfile.close()

    def _write_column_parameters_file(self):
        """
        get_column_parameters should return a dictionary where keys represent
        the column numbers. For example:
        {
            0: {'param1': 10, 'param2': 20},
            1: {'param1': 10, 'param2': 40}
        }
        """
        if self._write_flag:
            column_params = self.get_column_parameters()
            assert isinstance(column_params, dict)
            with open(os.path.join(self.run_dir, 'column_params.json'),
                      'w') as outfile:
                json.dump(column_params, outfile)
            outfile.close()

    def _shutdown_simulation(self):
        if self._write_flag:
            # Update meta file
            meta = json.load(open(os.path.join(self.run_dir,
                                               'meta.json'), 'r'))
            meta['completed'] = True
            meta['end_time'] = str(dt.datetime.utcnow())[:19]
            with open(os.path.join(self.run_dir, 'meta.json'),
                      'w') as outfile:
                json.dump(meta, outfile)

    def _import_run_meta_for_restart(self, run_name):
        self.run_dir = os.path.join(self._strategy_output_dir, run_name)
        assert os.path.isdir(self.run_dir), \
            '[{}] not available'.format(run_name)
        meta = json.load(open(os.path.join(self.run_dir,
                                           'meta.json'), 'r'))
        assert not meta['completed'], '[{}] completed'.format(run_name)
        # Set prepped_data_version
        self._prepped_data_dir = os.path.join(
            os.path.dirname(self._prepped_data_dir),
            meta['prepped_data_version'])
        self.strategy_output_dir = os.path.join(
                self.run_dir, 'index_outputs')

    def _get_max_run_time_index_for_restart(self):
        all_files = os.listdir(os.path.join(self.run_dir, 'index_outputs'))
        all_files = [x for x in all_files if x.find('_returns.csv') >= 0]
        self._max_run_time_index = len(all_files) - 1

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _print_prepped_data_meta(self):
        if self._gcp_implementation:
            blob = self._bucket.get_blob(
                os.path.join(self._prepped_data_dir, 'meta.json'))
            meta = json.loads(blob.download_as_string())
        else:
            meta = json.load(open(os.path.join(self._prepped_data_dir,
                                               'meta.json'), 'r'))

        print('\n## Meta data for {} - {} ##'.format(meta['strategy_name'],
                                                     meta['version']))
        if self.get_constructor_type() in ['etfs', 'ids']:
            print('IDs: {}'.format(
                meta['filter_args_ids']['ids']))
            print('Start Date: {}'.format(
                meta['filter_args_ids']['start_date']))
            print('End Date: {}\n'.format(
                meta['filter_args_ids']['end_date']))
        else:
            print('Filter variable: {}'.format(
                meta['filter_args_univ']['filter']))
            print('Where filter: {}'.format(
                meta['filter_args_univ']['where']))
            print('Universe size: {}\n'.format(
                meta['filter_args_univ']['univ_size']))
            print('Start Year: {}'.format(
                meta['date_parameters_univ']['start_year']))
            print('Train Period Length: {}'.format(
                meta['date_parameters_univ']['train_period_length']))
            print('Test Period Length: {}'.format(
                meta['date_parameters_univ']['test_period_length']))
            print('Universe Creation Frequency: {}'.format(
                meta['date_parameters_univ']['frequency']))

    def _get_prepped_data_file_names(self):

        if self._gcp_implementation:
            all_files = [x.name for x in self._bucket.list_blobs()]
            self._prepped_data_files = [
                x for x in all_files if x.startswith(
                    self._prepped_data_dir)]
        else:
            all_files = os.listdir(self._prepped_data_dir)
            self._prepped_data_files = [
                x for x in all_files if x[-8:] == 'data.csv']

    # ~~~~~~ To Be Overwritten ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def run_index(self, index):
        """
        Takes in integer
        """
        raise NotImplementedError('Strategy.run_index')

    @abstractmethod
    def get_column_parameters(self):
        """
        Takes in integer
        """
        raise NotImplementedError('Strategy.get_column_parameters')

    # ~~~~~~ DataConstructor Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def make_data(self, data_prep_version=None):
        """
        Parameters
        ----------
        data_prep_version : str
            Will restart data pull if present.
        """
        if data_prep_version:
            DataConstructor(self).run(
                rerun_version=data_prep_version)
        else:
            DataConstructor(self).run()

    @abstractmethod
    def get_features(self):
        raise NotImplementedError('Strategy.get_features')

    def get_constructor_type(self):
        return 'universe'

    def get_ids_filter_args(self):
        return {
            'ids': [],
            'start_date': '2010-01-01',
            'end_date': '2015-01-01'}

    def get_univ_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': 500}

    def get_univ_date_parameters(self):
        """
        Parameters
        ----------
        frequency : str
            'Q' for quarter and 'M' for monthly
        train_period_length : int
            Number of periods (quarters or months) to provide
            training data for. Training and test data are flagged as a
            column in the data
        test_period_length : int
            Number of periods to provide test data for going forward.
            The frequency indicates how often one gets new universe
            data, but this could extend into the future if the data
            was necessary.
        start_year : int
            Year
        """
        return {
            'frequency': 'Q',
            'train_period_length': 4,
            'test_period_length': 1,
            'start_year': 2007
        }

    # ~~~~~~ To Be Used by Derived Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def read_data_from_index(self, index):
        if not hasattr(self, '_prepped_data_files'):
            # This is used for interactive development so get data files
            # doesn't need to be manually called
            self._get_prepped_data_file_names()
        if self._gcp_implementation:
            blob = self._bucket.get_blob(self._prepped_data_files[index])
            data = pd.read_csv(StringIO(blob.download_as_string()))
        else:
            dpath = os.path.join(self._prepped_data_dir,
                                 self._prepped_data_files[index])
            data = pd.read_csv(dpath)
        data.Date = convert_date_array(data.Date)
        data.SecCode = data.SecCode.astype(int).astype(str)
        return data

    def write_index_results(self, returns_df, index, suffix='returns'):
        """
        This is a wrapper function for cloud implementation.
        """
        if self._gcp_implementation:
            output_name = self._prepped_data_files[index].split('/')[-1]
        else:
            output_name = self._prepped_data_files[index]
        output_name = output_name.replace('data', suffix)
        if self._write_flag:
            returns_df.to_csv(os.path.join(self.strategy_output_dir,
                                           output_name))

    def write_index_stats(self, stats, index):
        output_name = self._prepped_data_files[index]
        output_name = output_name.replace('data.csv', 'stats.json')
        if self._write_flag:
            with open(os.path.join(self.strategy_output_dir,
                                   output_name), 'w') as outfile:
                json.dump(stats, outfile)
            outfile.close()


def copytree(src, dst, symlinks=False, ignore=None):
    os.mkdir(dst)
    for item in os.listdir(src):
        if item.find('.pyc') > 0:
            continue
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


# ~~~~~~  Make ArgParser  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_argument_parser(Strategy):

    import argparse
    from ram.data.data_constructor import print_strategy_versions
    from ram.data.data_constructor import print_strategy_meta
    from ram.data.data_constructor import clean_directory
    from ram.data.data_constructor import get_version_name
    from ram.analysis.run_manager import RunManager

    parser = argparse.ArgumentParser()

    # Data Exploration Commands
    parser.add_argument(
        '-lv', '--list_versions', action='store_true',
        help='List all versions of prepped data for a strategy')
    parser.add_argument(
        '-lr', '--list_runs', action='store_true',
        help='List all simulations for a strategy')
    parser.add_argument(
        '-pm', '--print_meta', type=str,
        help='Print meta data. i.e version_0001 or Key val')
    parser.add_argument(
        '-cv', '--clean_version', type=str,
        help='Delete version. i.e version_0001 or Key val')

    # Simulation Commands
    parser.add_argument(
        '-v', '--data_version',
        help='Version number of simulation.')
    parser.add_argument(
        '-w', '--write_simulation', action='store_true',
        help='Write simulation')
    parser.add_argument(
        '-s', '--simulation', action='store_true',
        help='Run simulation for debugging')
    parser.add_argument(
        '--description', default=None,
        help='Run description. Used namely in a batch file')
    parser.add_argument(
        '--cloud', action='store_true',
        help='Tag must be added for GCP implementation')
    parser.add_argument(
        '--restart_run', type=str, default=None,
        help='If something craps out, use this tag. Send in run name'
    )

    # Data Construction Commands
    parser.add_argument(
        '-d', '--data_prep', type=str,
        help='Run DataConstructor. To create new data version, use arg '
             '-1, else to restart use version name or key val, i.e. '
             'version_0001 or Key val')

    args = parser.parse_args()

    # Data Exploration
    if args.list_versions:
        print_strategy_versions(Strategy.__name__)
    elif args.list_runs:
        runs = RunManager.get_run_names(Strategy.__name__)
        # Adjust column width
        runs['Description'] = runs.Description.apply(lambda x: x[:20] + ' ...')
        print(runs)
    elif args.print_meta:
        version = get_version_name(Strategy.__name__, args.print_meta)
        print_strategy_meta(Strategy.__name__, version)
    elif args.clean_version:
        version = get_version_name(Strategy.__name__, args.clean_version)
        clean_directory(Strategy.__name__, version)

    # Simulation Commands
    elif args.restart_run:
        strategy = Strategy(gcp_implementation=args.cloud)
        strategy.restart(args.restart_run)
    elif args.write_simulation:
        if not args.data_version:
            print('Data version must be provided')
        else:
            version = get_version_name(Strategy.__name__, args.data_version)
            strategy = Strategy(version, True, gcp_implementation=args.cloud)
            strategy.start(args.description)
    elif args.simulation:
        if not args.data_version:
            print('Data version must be provided')
        else:
            import ipdb; ipdb.set_trace()
            version = get_version_name(Strategy.__name__, args.data_version)
            strategy = Strategy(version, False, gcp_implementation=args.cloud)
            strategy.start()

    # Data Construction
    elif args.data_prep:
        if args.data_prep != '-1':
            version = get_version_name(Strategy.__name__, args.data_prep)
            Strategy(prepped_data_version=version).make_data(version)
        else:
            Strategy().make_data()
