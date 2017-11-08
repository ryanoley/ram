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
            self._strategy_output_dir = os.path.join('simulations',
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
        """
        Creates the directory structure for the output AND CRUCIALLY
        sets the run_dir. This implementation has been reworked for gcp.
        """
        # Get run names
        if self._gcp_implementation:
            all_files = [x.name for x in self._bucket.list_blobs()]
            all_files = [x for x in all_files if x.startswith(
                self._strategy_output_dir)]
            strip_str = self._strategy_output_dir + '/'
            all_files = [x.replace(strip_str, '') for x in all_files]
            all_files = [x for x in all_files if x.find('run') >= 0]
            new_ind = int(max(all_files).split('/')[0].split('_')[1]) + 1 \
                if all_files else 1
        elif os.path.isdir(self._strategy_output_dir):
            all_dirs = [x for x in os.listdir(
                self._strategy_output_dir) if x[:3] == 'run']
            new_ind = int(max(all_dirs).split('_')[1]) + 1 if all_dirs else 1
        elif self._write_flag:
            os.makedirs(self._strategy_output_dir)
            new_ind = 1
        else:
            new_ind = 1
        # Get all run versions for increment for this run
        self.run_dir = os.path.join(self._strategy_output_dir,
                                    'run_{0:04d}'.format(new_ind))
        # Output directory setup
        self.strategy_output_dir = os.path.join(self.run_dir, 'index_outputs')
        if self._write_flag and not self._gcp_implementation:
            os.mkdir(self.run_dir)
            os.makedirs(self.strategy_output_dir)

    def _copy_source_code(self):
        if self._write_flag and not self._gcp_implementation:
            # Copy source code for Strategy
            source_path = os.path.dirname(inspect.getabsfile(self.__class__))
            dest_path = os.path.join(self.run_dir, 'strategy_source_copy')
            copytree(source_path, dest_path)
        elif self._write_flag and self._gcp_implementation:
            source_path = os.path.dirname(inspect.getabsfile(self.__class__))
            dest_path = os.path.join(self.run_dir, 'strategy_source_copy')
            copy_string = 'gsutil -q -m cp -r {} gs://{}/{}'.format(
                source_path, config.GCP_STORAGE_BUCKET_NAME, dest_path)
            os.system(copy_string)

    def _create_meta_file(self, user_description=None):
        if self._write_flag:
            # To aid unittest
            desc = user_description if user_description else \
                prompt_for_description()
            git_branch, git_commit = get_git_branch_commit()
            # Create meta object
            meta = {
                'prepped_data_version': self._data_version,
                'latest_git_commit': git_commit,
                'git_branch': git_branch,
                'description': desc,
                'completed': False,
                'start_time': str(dt.datetime.utcnow())[:19]
            }
            out_path = os.path.join(self.run_dir, 'meta.json')
            if self._gcp_implementation:
                write_json_cloud(meta, out_path, self._bucket)
            else:
                write_json(meta, out_path)

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
            out_path = os.path.join(self.run_dir, 'column_params.json')
            if self._gcp_implementation:
                write_json_cloud(column_params, out_path, self._bucket)
            else:
                write_json(column_params, out_path)

    def _shutdown_simulation(self):
        if self._write_flag:
            # Update meta file
            meta_file_path = os.path.join(self.run_dir, 'meta.json')
            if self._gcp_implementation:
                meta = read_json_cloud(meta_file_path, self._bucket)
            else:
                meta = read_json(meta_file_path)
            meta['completed'] = True
            meta['end_time'] = str(dt.datetime.utcnow())[:19]
            if self._gcp_implementation:
                write_json_cloud(meta, meta_file_path, self._bucket)
            else:
                write_json(meta, meta_file_path)

    def _import_run_meta_for_restart(self, run_name):
        self.run_dir = os.path.join(self._strategy_output_dir, run_name)
        meta_file_path = os.path.join(self.run_dir, 'meta.json')
        if self._gcp_implementation:
            meta = read_json_cloud(meta_file_path, self._bucket)
        else:
            meta = read_json(meta_file_path)
        # Set prepped_data_version
        self._prepped_data_dir = os.path.join(
            os.path.dirname(self._prepped_data_dir),
            meta['prepped_data_version'])
        self.strategy_output_dir = os.path.join(
                self.run_dir, 'index_outputs')

    def _get_max_run_time_index_for_restart(self):
        if self._gcp_implementation:
            all_files = [x.name for x in self._bucket.list_blobs()]
            all_files = [x for x in all_files if x.find(self.run_dir) >= 0]
            all_files = [x for x in all_files if x.find('_returns.csv') >= 0]
            all_files = [x.replace(self.run_dir+'/index_outputs/', '')
                         for x in all_files]
        else:
            all_files = os.listdir(os.path.join(self.run_dir, 'index_outputs'))
            all_files = [x for x in all_files if x.find('_returns.csv') >= 0]
        self._max_run_time_index = len(all_files) - 1
        # Delete final file if it isn't same as matching raw data file
        last_run_file = max(all_files)
        run_path = os.path.join(self.run_dir, 'index_outputs', last_run_file)
        data_path = os.path.join(self._prepped_data_dir,
                                 '{}_data.csv'.format(last_run_file[:8]))
        if self._gcp_implementation:
            rdata = read_csv_cloud(run_path, self._bucket)
            ddata = read_csv_cloud(data_path, self._bucket)
        else:
            rdata = pd.read_csv(run_path, index_col=0)
            ddata = pd.read_csv(data_path)
        max_run_file_date = convert_date_array(rdata.index).max()
        max_data_file_date = convert_date_array(ddata.Date).max()
        # Check if
        if max_run_file_date < max_data_file_date:
            self._max_run_time_index -= 1
            if self._gcp_implementation:
                blob = self._bucket.blob(run_path)
                blob.delete()
            else:
                os.remove(run_path)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _print_prepped_data_meta(self):
        meta_path = os.path.join(self._prepped_data_dir, 'meta.json')
        if self._gcp_implementation:
            meta = read_json_cloud(meta_path, self._bucket)
        else:
            meta = read_json(meta_path)

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
            all_files = [x for x in all_files
                         if x.startswith(self._prepped_data_dir)]
            strip_str = self._prepped_data_dir + '/'
            all_files = [x.replace(strip_str, '') for x in all_files]
            self._prepped_data_files = [x for x in all_files
                                        if x.find('_data.csv') > 0]
            self._prepped_data_files = [
                x for x in self._prepped_data_files
                if x.find('market_index_data') == -1]
        else:
            all_files = os.listdir(self._prepped_data_dir)
            self._prepped_data_files = [
                x for x in all_files if x[-8:] == 'data.csv']
            self._prepped_data_files = [
                x for x in self._prepped_data_files
                if x.find('market_index_data') == -1]
        self._prepped_data_files.sort()

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

    def make_market_index_data(self, data_prep_version):
        """
        Parameters
        ----------
        data_prep_version : str
            Will restart data pull if present.
        """
        DataConstructor(self).run_index_data(data_prep_version)

    @abstractmethod
    def get_features(self):
        raise NotImplementedError('Strategy.get_features')

    def get_market_index_data_arguments(self):
        return {
            'features': ['AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose',
                         'VOL10_AdjClose', 'VOL20_AdjClose',
                         'RSI10_AdjClose', 'RSI20_AdjClose',
                         'BOLL10_AdjClose', 'BOLL20_AdjClose'],
            'seccodes': [50311, 61258, 61259, 11097, 11099, 11100, 10955,
                         11101, 11102, 11096, 11103, 11104, 11113,
                         11132814, 10922530]
        }

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
            'univ_size': 10}

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
            self._get_prepped_data_file_names()
        dpath = os.path.join(self._prepped_data_dir,
                             self._prepped_data_files[index])
        if self._gcp_implementation:
            data = read_csv_cloud(dpath, self._bucket)
        else:
            data = pd.read_csv(dpath)
        data.Date = convert_date_array(data.Date)
        data.SecCode = data.SecCode.astype(int).astype(str)
        return data

    def read_market_index_data(self):
        dpath = os.path.join(self._prepped_data_dir,
                             'market_index_data.csv')
        if self._gcp_implementation:
            data = read_csv_cloud(dpath, self._bucket)
        else:
            data = pd.read_csv(dpath)
        data.Date = convert_date_array(data.Date)
        data.SecCode = data.SecCode.astype(int).astype(str)
        return data

    def write_index_results(self, returns_df, index, suffix='returns'):
        """
        This is a wrapper function for cloud implementation.
        """
        output_name = self._prepped_data_files[index].replace('data', suffix)
        output_path = os.path.join(self.strategy_output_dir, output_name)
        if self._write_flag and self._gcp_implementation:
            to_csv_cloud(returns_df, output_path, self._bucket)
        elif self._write_flag:
            returns_df.to_csv(output_path)

    def write_index_stats(self, stats, index):
        output_name = self._prepped_data_files[index].replace(
            'data.csv', 'stats.json')
        output_path = os.path.join(self.strategy_output_dir, output_name)
        if self._write_flag and self._gcp_implementation:
            write_json_cloud(stats, output_path, self._bucket)
        elif self._write_flag:
            write_json(stats, output_path)


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


# ~~~~~~ Read/Write functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def write_json(out_dictionary, path):
    assert isinstance(out_dictionary, dict)
    with open(path, 'w') as outfile:
        json.dump(out_dictionary, outfile)


def read_json(path):
    return json.load(open(path, 'r'))


def write_json_cloud(out_dictionary, path, bucket):
    assert isinstance(out_dictionary, dict)
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(out_dictionary))


def read_json_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return json.loads(blob.download_as_string())


def read_csv_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return pd.read_csv(StringIO(blob.download_as_string()))


def to_csv_cloud(data, path, bucket):
    blob = bucket.blob(path)
    blob.upload_from_string(data.to_csv())


# ~~~~~~  Make ArgParser  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_argument_parser(Strategy):

    import argparse
    from ram.data.data_constructor import print_strategy_versions
    from ram.data.data_constructor import print_strategy_meta
    from ram.data.data_constructor import clean_directory
    from ram.data.data_constructor import get_version_name
    from ram.data.data_constructor import get_version_name_cloud
    from ram.analysis.run_manager import get_run_data

    parser = argparse.ArgumentParser()

    # Cloud tag must be included anytime working in GCP
    parser.add_argument(
        '-c', '--cloud', action='store_true',
        help='Tag must be added for GCP implementation')

    # Data Exploration Commands
    parser.add_argument(
        '-lv', '--list_versions', action='store_true',
        help='List all versions of prepped data for a strategy')
    parser.add_argument(
        '-pvm', '--print_version_meta', type=str,
        help='Print meta data for version, i.e version_0001 or Key val')
    parser.add_argument(
        '-cv', '--clean_version', type=str,
        help='Delete version. i.e version_0001 or Key val')
    parser.add_argument(
        '-lr', '--list_runs', action='store_true',
        help='List all simulations for a strategy')

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
        '-d', '--description', default=None,
        help='Run description. Used namely in a batch file')
    parser.add_argument(
        '-r', '--restart_run', type=str, default=None,
        help='If something craps out, use this tag. Send in run name'
    )

    # Data Construction Commands
    parser.add_argument(
        '-dp', '--data_prep', type=str,
        help='Run DataConstructor. To create new data version, use arg '
             '-1, else to restart use version name or key val, i.e. '
             'version_0001 or Key val')

    parser.add_argument(
        '-dm', '--market_data', type=str,
        help='Market data added to directory for version. Input version '
             'or Key val')

    args = parser.parse_args()

    # ~~~~~~ DATA EXPLORATION ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if args.list_versions:
        print_strategy_versions(Strategy.__name__, args.cloud)

    elif args.print_version_meta:
        print_strategy_meta(Strategy.__name__,
                            args.print_version_meta,
                            args.cloud)

    elif args.clean_version:
        clean_directory(Strategy.__name__, args.clean_version)

    elif args.list_runs:
        runs = get_run_data(Strategy.__name__, args.cloud)
        # Adjust column width
        runs['Description'] = runs.Description.apply(lambda x: x[:20] + ' ...')
        print(runs)

    # ~~~~~~ SIMULATION COMMANDS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    elif args.restart_run:
        runs = get_run_data(Strategy.__name__, args.cloud)
        if args.restart_run in runs.Run.values:
            version = args.restart_run
        else:
            version = runs.Run.iloc[int(args.restart_run)]
        strategy = Strategy(gcp_implementation=args.cloud, write_flag=True)
        import pdb; pdb.set_trace()
        strategy.restart(version)

    elif args.write_simulation:
        if not args.data_version:
            print('Data version must be provided')
            return
        if args.cloud:
            version = get_version_name_cloud(Strategy.__name__,
                                             args.data_version)
        else:
            version = get_version_name(Strategy.__name__, args.data_version)
        # Start simulation
        strategy = Strategy(prepped_data_version=version,
                            write_flag=True,
                            gcp_implementation=args.cloud)
        strategy.start(args.description)

    elif args.simulation:
        if not args.data_version:
            print('Data version must be provided')
            return
        if args.cloud:
            version = get_version_name_cloud(Strategy.__name__,
                                             args.data_version)
        else:
            version = get_version_name(Strategy.__name__,
                                       args.data_version)
        import ipdb; ipdb.set_trace()
        strategy = Strategy(prepped_data_version=version,
                            gcp_implementation=args.cloud)
        strategy.start()

    # ~~~~~~ DATA CONSTRUCTION ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    elif args.data_prep:
        if args.data_prep != '-1':
            version = get_version_name(Strategy.__name__, args.data_prep)
            Strategy(prepped_data_version=version).make_data(version)
        else:
            Strategy().make_data()

    elif args.market_data:
        version = get_version_name(Strategy.__name__, args.market_data)
        Strategy().make_market_index_data(version)
