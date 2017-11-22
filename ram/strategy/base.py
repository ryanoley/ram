import os
import sys
import json
import shutil
import inspect
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt

from sklearn.externals import joblib

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

    # ~~~~~~ To Be Overwritten ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def strategy_init(self):
        """
        This will be invoked upon start or restart
        """
        raise NotImplementedError('Strategy.strategy_init')

    @abstractmethod
    def get_data_blueprint_container(self):
        """
        Should return a dictionary with Blueprints in values and any
        labels as keys.
        """
        raise NotImplementedError('Strategy.get_data_blueprints')

    @abstractmethod
    def get_strategy_source_versions(self):
        """
        Should return a dictionary with descriptions in values and any
        labels as keys.
        """
        raise NotImplementedError('Strategy.get_strategy_source_versions')

    @abstractmethod
    def process_raw_data(self, data, time_index, market_data=None):
        """
        TODO: docs
        """
        raise NotImplementedError('Strategy.process_raw_data')

    @abstractmethod
    def run_index(self, index):
        """
        TODO: docs
        """
        raise NotImplementedError('Strategy.run_index')

    @abstractmethod
    def get_column_parameters(self):
        """
        TODO: docs
        """
        raise NotImplementedError('Strategy.get_column_parameters')

    @abstractmethod
    def implementation_training(self):
        """
        This function should simply be used to load the names of the
        parameters that need to be training.

        Load through: implementation_training_cache_params

        Input should be list with items in the following format:
        `Strategy_run_RUNNUM_COLNUM`, as they are printed out from
        combo_search.
        """
        raise NotImplementedError('Strategy.implementation_training')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self,
                 strategy_code_version=None,
                 prepped_data_version=None,
                 write_flag=False,
                 ram_prepped_data_dir=config.PREPPED_DATA_DIR,
                 ram_simulations_dir=config.SIMULATIONS_DATA_DIR,
                 ram_implementation_dir=config.IMPLEMENTATION_DATA_DIR):
        """
        Parameters
        ----------
        strategy_code_version : str
            A parameter that can be used to select different versions of
            the strategy code
        prepped_data_version : str
            This is the name of the prepped data version, e.g.: version_0002

        write_flag : bool
            Whether to create an output directory and write results to file

        ram_prepped_data_dir : str
            Location of the global prepped data directory, not specific to the
            Strategy or version provided. Defaults to what is in the global
            config file
        ram_simulations_dir : str
            Location where written output will go. Defaults to what is in
            the global config file
        ram_implementation_dir : str
            Location where implementation model training output will go.
        """
        self.strategy_code_version = strategy_code_version
        self.prepped_data_version = prepped_data_version
        self._write_flag = write_flag
        # Base ram directories for data
        self._ram_prepped_data_dir = ram_prepped_data_dir
        self._ram_simulations_dir = ram_simulations_dir
        self._ram_implementation_dir = ram_implementation_dir
        self._init_gcp_implementation()
        self._init_prepped_data_dir()
        self._init_simulations_output_dir()
        self._init_implementation_dir()

    # ~~~~~~ RUN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def start(self, description=None):
        self.strategy_init()
        self._get_prepped_data_file_names()
        self._create_run_output_dir()
        self._copy_source_code()
        self._create_meta_file(description)
        self._write_column_parameters_file()
        market_data = self.read_market_index_data()
        for time_index in tqdm(range(len(self._prepped_data_files))):
            self.process_raw_data(
                self.read_data_from_index(time_index),
                time_index,
                market_data.copy())
            self.run_index(time_index)
        self._shutdown_simulation()
        return

    def restart(self, run_name):
        self.strategy_init()
        self._import_run_meta_for_restart(run_name)
        self._get_prepped_data_file_names()
        self._get_max_run_time_index_for_restart()
        market_data = self.read_market_index_data()
        for time_index in tqdm(range(len(self._prepped_data_files))):
            self.process_raw_data(
                self.read_data_from_index(time_index),
                time_index,
                market_data.copy())
            if time_index < self._restart_time_index:
                continue
            self.run_index(time_index)
        self._shutdown_simulation()
        return

    # ~~~~~~ GCP ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _init_gcp_implementation(self):
        self._gcp_implementation = config.GCP_CLOUD_IMPLEMENTATION
        # Only connect to GCP instance if prepped data is there
        if self._gcp_implementation:
            self._gcp_client = storage.Client()
            self._gcp_bucket = self._gcp_client.get_bucket(
                config.GCP_STORAGE_BUCKET_NAME)
        return

    # ~~~~~~ Paths to files ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _init_prepped_data_dir(self):
        # If no prepped data was assigned, there will be no calling of start
        if self.prepped_data_version is None:
            return
        if self._gcp_implementation:
            path = os.path.join('prepped_data',
                                self.__class__.__name__,
                                self.prepped_data_version)
        else:
            path = os.path.join(self._ram_prepped_data_dir,
                                self.__class__.__name__,
                                self.prepped_data_version)
        self.data_version_dir = path

    def _init_simulations_output_dir(self):
        if self._gcp_implementation:
            path = os.path.join('simulations',
                                self.__class__.__name__)
        else:
            path = os.path.join(self._ram_simulations_dir,
                                self.__class__.__name__)
        self._strategy_output_dir = path

    def _init_implementation_dir(self):
        if self._gcp_implementation:
            path = os.path.join('implementation',
                                self.__class__.__name__,
                                'trained_models')
        else:
            path = os.path.join(self._ram_implementation_dir,
                                self.__class__.__name__,
                                'trained_models')
        self._strategy_implementation_model_dir = path

    # ~~~~~~ Implementation Training Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def implementation_training_prep(self, top_params):
        """
        param_name, run_name, strategy_version, data_version
        """
        top_params = clean_top_params(top_params)
        output = pd.DataFrame(columns=['param_name', 'run_name',
                                       'strategy_version', 'data_version',
                                       'column_name'])
        for i, (param, run) in enumerate(zip(*top_params)):
            output.loc[i, 'param_name'] = param
            output.loc[i, 'run_name'] = run
            meta = self._import_run_meta(run)
            output.loc[i, 'strategy_version'] = meta['strategy_code_version']
            output.loc[i, 'data_version'] = meta['prepped_data_version']
            output.loc[i, 'column_name'] = param.split('_')[-1]
        # Unique versions of strategy/data need their own data stack
        output['stack_index'] = 0
        for i in range(len(output)):
            output.loc[i, 'stack_index'] = '{}~{}'.format(
                output.loc[i, 'strategy_version'],
                output.loc[i, 'data_version'])
        output = output.sort_values('stack_index').reset_index(drop=True)
        return output

    def implementation_training_stack_version_data(self, data_version):
        """
        To be used by derived class to prep data
        """
        self.prepped_data_version = data_version
        self._init_prepped_data_dir()
        self._get_prepped_data_file_names()
        market_data = self.read_market_index_data()
        for time_index in tqdm(range(len(self._prepped_data_files))):
            self.process_raw_data(
                self.read_data_from_index(time_index),
                time_index,
                market_data.copy())
        return

    def import_run_column_params(self, run_name):
        path = os.path.join(self._strategy_output_dir,
                            run_name,
                            'column_params.json')
        if self._gcp_implementation:
            column_params = read_json_cloud(path, self._gcp_bucket)
        else:
            column_params = read_json(path)
        return column_params

    def implementation_training_write_params_model(self,
                                                   run_name,
                                                   params,
                                                   model):
        # Set paths for output files
        model_cache_path = os.path.join(self.implementation_output_dir,
                                        run_name + '_skl_model.pkl')
        params_path = os.path.join(self.implementation_output_dir,
                                   run_name + '_params.json')
        # Write
        if self._gcp_implementation:
            blob = self._bucket.blob(model_cache_path)
            blob.upload_from_string(pickle.dumps(model))
            write_json_cloud(params, params_path, self._gcp_bucket)
        else:
            joblib.dump(model, model_cache_path)
            write_json(params, params_path)
        return

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _create_run_output_dir(self):
        """
        Creates the directory structure for the output and crucially
        sets the `strategy_run_output_dir`.
        """
        if not self._write_flag:
            return
        # Get all run versions for increment for this run
        if self._gcp_implementation:
            all_files = [x.name for x in self._gcp_bucket.list_blobs()]
            all_files = [x for x in all_files if x.startswith(
                self._strategy_output_dir)]
            strip_str = self._strategy_output_dir + '/'
            all_files = [x.replace(strip_str, '') for x in all_files]
            all_files = [x for x in all_files if x.find('run') >= 0]
            new_ind = int(max(all_files).split('/')[0].split('_')[1]) + 1 \
                if all_files else 1
            path = os.path.join(
                self._strategy_output_dir, 'run_{0:04d}'.format(new_ind))
        else:
            # Check if directory structure exists
            if not os.path.isdir(self._ram_simulations_dir):
                os.mkdir(self._ram_simulations_dir)
            if not os.path.isdir(self._strategy_output_dir):
                os.mkdir(self._strategy_output_dir)
            # Search for new ind
            all_dirs = [x for x in os.listdir(
                self._strategy_output_dir) if x[:3] == 'run']
            new_ind = int(max(all_dirs).split('_')[1]) + 1 if all_dirs else 1
            path = os.path.join(
                self._strategy_output_dir, 'run_{0:04d}'.format(new_ind))
            # Create directories
            os.mkdir(path)
            os.mkdir(os.path.join(path, 'index_outputs'))

        self.strategy_run_output_dir = path

    def _create_implementation_output_dir(self):
        """
        Creates the directory structure for implementation output
        """
        if not self._write_flag:
            return
        # Get run names
        if self._gcp_implementation:
            all_files = [x.name for x in self._gcp_bucket.list_blobs() if
                         x.name.find(
                             self._strategy_implementation_model_dir) > -1]
            # TODO: get new_ind
            new_ind = 1
            path = os.path.join(self._strategy_implementation_model_dir,
                                'models_{0:04d}'.format(new_ind))
        else:
            # Check if directory structure exists
            if not os.path.isdir(self._ram_implementation_dir):
                os.mkdir(self._ram_implementation_dir)
            path = os.path.join(self._ram_implementation_dir,
                                self.__class__.__name__)
            if not os.path.isdir(path):
                os.mkdir(path)
            path = os.path.join(path, 'trained_models')
            if not os.path.isdir(path):
                os.mkdir(path)
            # Search for new ind
            all_dirs = [x for x in os.listdir(
                self._strategy_implementation_model_dir) if x[:7] == 'models_']
            new_ind = int(max(all_dirs).split('_')[1]) + 1 if all_dirs else 1
            path = os.path.join(self._strategy_implementation_model_dir,
                                'models_{0:04d}'.format(new_ind))
            os.mkdir(path)
        self.implementation_output_dir = path

    def _copy_source_code(self):
        if self._write_flag and not self._gcp_implementation:
            # Copy source code for Strategy
            source_path = os.path.dirname(inspect.getabsfile(self.__class__))
            dest_path = os.path.join(self.strategy_run_output_dir,
                                     'strategy_source_copy')
            copytree(source_path, dest_path)
        elif self._write_flag and self._gcp_implementation:
            source_path = os.path.dirname(inspect.getabsfile(self.__class__))
            dest_path = os.path.join(self.strategy_run_output_dir,
                                     'strategy_source_copy')
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
                'prepped_data_version': self.prepped_data_version,
                'strategy_code_version': self.strategy_code_version,
                'latest_git_commit': git_commit,
                'git_branch': git_branch,
                'description': desc,
                'completed': False,
                'start_time': str(dt.datetime.utcnow())[:19]
            }
            out_path = os.path.join(self.strategy_run_output_dir, 'meta.json')
            if self._gcp_implementation:
                write_json_cloud(meta, out_path, self._gcp_bucket)
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
            out_path = os.path.join(self.strategy_run_output_dir,
                                    'column_params.json')
            if self._gcp_implementation:
                write_json_cloud(column_params, out_path, self._gcp_bucket)
            else:
                write_json(column_params, out_path)

    def _shutdown_simulation(self):
        if self._write_flag:
            # Update meta file
            meta_file_path = os.path.join(self.strategy_run_output_dir,
                                          'meta.json')
            if self._gcp_implementation:
                meta = read_json_cloud(meta_file_path, self._gcp_bucket)
            else:
                meta = read_json(meta_file_path)
            meta['completed'] = True
            meta['end_time'] = str(dt.datetime.utcnow())[:19]
            if self._gcp_implementation:
                write_json_cloud(meta, meta_file_path, self._gcp_bucket)
            else:
                write_json(meta, meta_file_path)

    def _import_run_meta(self, run_name):
        self.strategy_run_output_dir = os.path.join(
            self._strategy_output_dir, run_name)
        meta_file_path = os.path.join(
            self.strategy_run_output_dir, 'meta.json')
        if self._gcp_implementation:
            return read_json_cloud(meta_file_path, self._gcp_bucket)
        else:
            return read_json(meta_file_path)

    def _import_run_meta_for_restart(self, run_name):
        meta = self._import_run_meta(run_name)
        # Set prepped_data_version
        self.prepped_data_version = meta['prepped_data_version']
        self.strategy_code_version = meta['strategy_code_version']
        self._init_prepped_data_dir()

    def _get_max_run_time_index_for_restart(self):
        if self._gcp_implementation:
            all_files = [x.name for x in self._gcp_bucket.list_blobs()]
            all_files = [x for x in all_files
                         if x.find(self.strategy_run_output_dir) >= 0]
            all_files = [x for x in all_files if x.find('_returns.csv') >= 0]
            all_files = [
                x.replace(self.strategy_run_output_dir+'/index_outputs/', '')
                for x in all_files]
        else:
            all_files = os.listdir(os.path.join(self.strategy_run_output_dir,
                                                'index_outputs'))
            all_files = [x for x in all_files if x.find('_returns.csv') >= 0]
        # Delete final file if it isn't same as matching raw data file
        last_run_file = max(all_files)
        run_path = os.path.join(self.strategy_run_output_dir,
                                'index_outputs', last_run_file)
        data_path = os.path.join(self.data_version_dir,
                                 '{}_data.csv'.format(last_run_file[:8]))
        if self._gcp_implementation:
            rdata = read_csv_cloud(run_path, self._gcp_bucket)
            rdata = rdata.set_index(rdata.columns[0])
            rdata.index.name = None
            ddata = read_csv_cloud(data_path, self._gcp_bucket)
        else:
            rdata = pd.read_csv(run_path, index_col=0)
            ddata = pd.read_csv(data_path)
        max_run_file_date = convert_date_array(rdata.index).max()
        max_data_file_date = convert_date_array(ddata.Date).max()
        # Check if final file needs to be updated
        if max_run_file_date < max_data_file_date:
            # Pop from all_files
            all_files = all_files[:-1]
            if self._gcp_implementation:
                blob = self._gcp_bucket.blob(run_path)
                blob.delete()
            else:
                os.remove(run_path)
        if all_files:
            # Get restart index number number
            max_returns_data = max([int(x.split('_')[0]) for x in all_files])
            prepped_data_indexes = np.array([int(x.split('_')[0]) for x
                                             in self._prepped_data_files])
            # Check if run is necessary
            if max_returns_data == prepped_data_indexes[-1]:
                print('No updating of run necessary')
                sys.exit()
            self._restart_time_index = \
                sum(max_returns_data >= prepped_data_indexes)
        else:
            self._restart_time_index = 0

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_prepped_data_file_names(self):
        if self._gcp_implementation:
            all_files = [x.name for x in self._gcp_bucket.list_blobs()]
            all_files = [x for x in all_files
                         if x.startswith(self.data_version_dir)]
            strip_str = self.data_version_dir + '/'
            all_files = [x.replace(strip_str, '') for x in all_files]
            self._prepped_data_files = [x for x in all_files
                                        if x.find('_data.csv') > 0]
            self._prepped_data_files = [
                x for x in self._prepped_data_files
                if x.find('market_index_data') == -1]
        else:
            all_files = os.listdir(self.data_version_dir)
            self._prepped_data_files = [
                x for x in all_files if x[-8:] == 'data.csv']
            self._prepped_data_files = [
                x for x in self._prepped_data_files
                if x.find('market_index_data') == -1]
        self._prepped_data_files.sort()

    # ~~~~~~ To Be Used by Derived Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def read_data_from_index(self, index):
        if not hasattr(self, '_prepped_data_files'):
            self._get_prepped_data_file_names()
        dpath = os.path.join(self.data_version_dir,
                             self._prepped_data_files[index])
        if self._gcp_implementation:
            data = read_csv_cloud(dpath, self._gcp_bucket)
        else:
            data = pd.read_csv(dpath)
        data.Date = convert_date_array(data.Date)
        data.SecCode = data.SecCode.astype(int).astype(str)
        return data

    def read_market_index_data(self):
        try:
            dpath = os.path.join(self.data_version_dir,
                                 'market_index_data.csv')
            if self._gcp_implementation:
                data = read_csv_cloud(dpath, self._gcp_bucket)
            else:
                data = pd.read_csv(dpath)
            data.Date = convert_date_array(data.Date)
            data.SecCode = data.SecCode.astype(int).astype(str)
            return data
        except:
            return pd.DataFrame()

    def write_index_results(self, returns_df, index, suffix='returns'):
        """
        This is a wrapper function for cloud implementation.
        """
        output_name = self._prepped_data_files[index].replace('data', suffix)
        output_path = os.path.join(self.strategy_run_output_dir,
                                   'index_outputs', output_name)
        if self._write_flag and self._gcp_implementation:
            to_csv_cloud(returns_df, output_path, self._gcp_bucket)
        elif self._write_flag:
            returns_df.to_csv(output_path)

    def write_index_stats(self, stats, index):
        output_name = self._prepped_data_files[index].replace(
            'data.csv', 'stats.json')
        output_path = os.path.join(self.strategy_run_output_dir,
                                   'index_outputs', output_name)
        if self._write_flag and self._gcp_implementation:
            write_json_cloud(stats, output_path, self._gcp_bucket)
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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class StrategyVersionContainer(object):

    def __init__(self):
        self._index = 0
        self._versions = {}

    def add_version(self, version_name, description):
        self._versions[version_name] = {
            'description': description,
            'key': self._index,
        }
        self._index += 1

    def get_version_by_name_or_index(self, index):
        try:
            index = int(index)
        except:
            pass
        if isinstance(index, str):
            if index in self._versions:
                return index
            else:
                return None
        else:
            for k, b in self._versions.iteritems():
                if b['key'] == index:
                    return k
            return None

    def __repr__(self):
        out_string = ' ~~ Available Strategy Versions ~~\n'
        out_string += ' Key\tVersion\t\tDescription\n'
        out_string += ' ---\t-------\t\t-----------\n'
        keys = self._versions.keys()
        keys.sort()
        for k in keys:
            b = self._versions[k]
            out_string += ' [{}]\t{}\t{}\n'.format(
                b['key'], k, b['description'])
        return out_string


# ~~~~~~ Read/Write functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def write_json(out_dictionary, path):
    assert isinstance(out_dictionary, dict)
    with open(path, 'w') as outfile:
        json.dump(out_dictionary, outfile)


def read_json(path):
    return json.load(open(path, 'r'), object_hook=_byteify)


def write_json_cloud(out_dictionary, path, bucket):
    assert isinstance(out_dictionary, dict)
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(out_dictionary))


def read_json_cloud(path, bucket):
    blob = bucket.get_blob(path)
    # json.dumps is to get rid of unicode
    return json.loads(blob.download_as_string(), object_hook=_byteify)


def read_csv_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return pd.read_csv(StringIO(blob.download_as_string()))


def to_csv_cloud(data, path, bucket):
    blob = bucket.blob(path)
    blob.upload_from_string(data.to_csv())


def _byteify(data, ignore_dicts=False):
    # From stack exchange:  http://bit.ly/2zneXGP
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True):
            _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data


# ~~~~~~  Implementation  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clean_top_params(top_params):
    # Clean
    ind = top_params[0].split('_').index('run')
    split_runs = [y.split('_') for y in top_params]
    top_params = ['{}_{}_{}'.format(x[ind], x[ind+1], x[ind+2])
                  for x in split_runs]
    run_names = ['{}_{}'.format(x[ind], x[ind+1]) for x in split_runs]
    return top_params, run_names


# ~~~~~~  Make ArgParser  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_argument_parser(Strategy):

    import argparse

    from ram.data.data_constructor import get_data_version_name
    from ram.data.data_constructor import print_data_versions
    from ram.analysis.run_manager import get_run_data

    parser = argparse.ArgumentParser()

    # DataConstructor related functionality
    parser.add_argument(
        '-db', '--data_list_blueprints', action='store_true',
        help='List all Strategy data blueprints')
    parser.add_argument(
        '-dv', '--data_list_versions', action='store_true',
        help='List all Strategy data versions')
    parser.add_argument(
        '-d_make', '--data_make_from_blueprint', type=str,
        help='Runs DataConstructor and creates new version from blueprint. '
        'Input should be either name of blueprint or index number.')
    parser.add_argument(
        '-d_update', '--data_update_version', type=str,
        help='Runs DataConstructor and updates version. '
        'Input should be either name of version or index number.')

    # Strategy related functionality
    parser.add_argument(
        '-sv', '--strategy_list_source_versions', action='store_true',
        help='List all Strategy source code versions')
    parser.add_argument(
        '-sr', '--strategy_list_runs', action='store_true',
        help='List all Strategy runs')

    # Simulation functionality
    parser.add_argument(
        '-s', '--strategy_version', type=str,
        help='Strategy source code to be used in simulation. Simple string '
        'passed to derived strategy class')
    parser.add_argument(
        '-d', '--data_version', type=str,
        help='Strategy data version to be used in simulation. '
        'Input should be either name of version or index number.')
    parser.add_argument(
        '-w', '--write_flag', action='store_true',
        help='Write simulation')
    parser.add_argument(
        '-r', '--restart_run', action='store_true',
        help='Restart run')
    parser.add_argument(
        '-i', '--implementation_training', action='store_true',
        help='Restart run')
    parser.add_argument(
        '--description', default=None,
        help='Run description. Used namely in a batch file')

    args = parser.parse_args()

    # ~~~~~~ DATA/STRATEGY EXPLORATION ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    if args.data_list_blueprints:
        # TODO
        blueprints = Strategy().get_data_blueprint_container()
        print(blueprints)

    elif args.data_list_versions:
        print_data_versions(strategy_name=Strategy.__name__,
                            cloud_flag=config.GCP_CLOUD_IMPLEMENTATION)

    elif args.strategy_list_source_versions:
        # TODO
        versions = Strategy().get_strategy_source_versions()
        print(versions)

    elif args.strategy_list_runs:
        # TODO
        runs = get_run_data(Strategy.__name__,
                            config.GCP_CLOUD_IMPLEMENTATION)
        # Adjust column width
        runs['Description'] = runs.Description.apply(lambda x: x[:20] + ' ...')
        print(runs)

    # ~~~~~~ DATA CONSTRUCTION ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    elif args.data_make_from_blueprint:
        blueprints = Strategy().get_data_blueprint_container()
        blueprint = blueprints.get_blueprint_by_name_or_index(
            args.data_make_from_blueprint)
        DataConstructor().run(blueprint)

    elif args.data_update_version:
        update_data_version = get_data_version_name(
            strategy_name=Strategy.__name__,
            version_name=args.data_update_version,
            cloud_flag=config.GCP_CLOUD_IMPLEMENTATION)
        DataConstructor().rerun(Strategy.__name__, update_data_version)

    # ~~~~~~ SIMULATION COMMANDS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    elif args.strategy_version and args.data_version:

        strategy_versions = Strategy().get_strategy_source_versions()
        strategy_version = strategy_versions.get_version_by_name_or_index(
            args.strategy_version)

        data_version = get_data_version_name(
            strategy_name=Strategy.__name__,
            version_name=args.data_version,
            cloud_flag=config.GCP_CLOUD_IMPLEMENTATION)

        strategy = Strategy(strategy_code_version=strategy_version,
                            prepped_data_version=data_version,
                            write_flag=args.write_flag)

        if not args.write_flag:
            import pdb; pdb.set_trace()

        strategy.start(args.description)

    elif args.restart_run:
        run_name = get_run_name(args.restart_run)
        strategy = Strategy(write_flag=True)
        strategy.restart(run_name)

    elif args.implementation_training:
        strategy = Strategy(write_flag=args.write_flag)
        strategy._create_implementation_output_dir()
        strategy.implementation_training()
