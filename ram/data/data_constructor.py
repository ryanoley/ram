import os
import json
import shutil
import itertools
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt

from google.cloud import storage

from gearbox import convert_date_array

from ram import config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.data.data_constructor_blueprint import DataConstructorBlueprint

from ram.utils.documentation import get_git_branch_commit


class DataConstructor(object):

    def __init__(self,
                 ram_prepped_data_dir=config.PREPPED_DATA_DIR,
                 ram_implementation_dir=config.IMPLEMENTATION_DATA_DIR):
        self._prepped_data_dir = ram_prepped_data_dir
        self._implementation_dir = ram_implementation_dir

    # ~~~~~~ Interface ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run(self, blueprint):
        self._check_parameters(blueprint)
        self._init_run(blueprint)
        self._make_output_directory(blueprint)
        self._write_archive_meta_data(blueprint)
        self._make_data(blueprint)

    def rerun(self, output_dir_name, rerun_version):
        blueprint = self._init_rerun(output_dir_name, rerun_version)
        self._check_file_completeness(blueprint)
        self._make_data(blueprint)

    def run_live(self, blueprint, strategy_name):
        self._check_parameters(blueprint)
        self._init_run_live(strategy_name)
        self._make_data(blueprint)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_data(self, blueprint):

        dh = DataHandlerSQL()

        # Market data - Do first to facilitate dev while doing data pull
        if hasattr(blueprint, 'market_data_params'):
            params = blueprint.market_data_params
            data = dh.get_index_data(
                seccodes=params['seccodes'],
                features=params['features'],
                start_date='1990-01-01',
                end_date='2050-04-01')
            self._clean_and_write_output(data, 'market_index_data.csv')

        if blueprint.constructor_type == 'etfs':
            data = dh.get_etf_data(
                tickers=blueprint.etfs_filter_arguments['tickers'],
                features=blueprint.features,
                start_date=blueprint.etfs_filter_arguments['start_date'],
                end_date=blueprint.etfs_filter_arguments['end_date'])
            file_name = '{}.csv'.format(
                blueprint.etfs_filter_arguments['output_file_name'])
            self._clean_and_write_output(data, file_name)

        elif blueprint.constructor_type == 'seccodes':
            data = dh.get_seccode_data(
                seccodes=blueprint.seccodes_filter_arguments['seccodes'],
                features=blueprint.features,
                start_date=blueprint.seccodes_filter_arguments['start_date'],
                end_date=blueprint.seccodes_filter_arguments['end_date'])
            file_name = '{}.csv'.format(
                blueprint.seccodes_filter_arguments['output_file_name'])
            self._clean_and_write_output(data, file_name)

        elif blueprint.constructor_type == 'indexes':
            pass

        elif blueprint.constructor_type == 'universe':
            date_iterator = self._make_date_iterator(blueprint)
            created_files = []
            for t1, t2, t3 in tqdm(date_iterator):
                # Check if file already exists in output directory
                file_name = '{}_data.csv'.format(t2.strftime('%Y%m%d'))
                if file_name in self._version_files:
                    continue
                created_files.append(file_name)
                # Otherwise pull and process data
                adj_filter_date = t2 - dt.timedelta(days=1)
                data = dh.get_filtered_univ_data(
                    features=blueprint.features,
                    start_date=t1,
                    end_date=t3,
                    filter_date=adj_filter_date,
                    filter_args=blueprint.universe_filter_arguments)
                data['TestFlag'] = data.Date > adj_filter_date
                if len(data[data.TestFlag]) == 0:
                    continue
                self._clean_and_write_output(data, file_name)
            # Update meta params
            max_train_date = data.Date[~data.TestFlag].max()
            self._update_meta_file(max_train_date, created_files)

        # HACK
        elif blueprint.constructor_type == 'universe_live':
            t1, t2, t3 = self._make_implementation_dates(blueprint)
            adj_filter_date = t2 - dt.timedelta(days=1)
            data = dh.get_filtered_univ_data(
                features=blueprint.features,
                start_date=t1,
                end_date=t3,
                filter_date=adj_filter_date,
                filter_args=blueprint.universe_filter_arguments)
            data['TestFlag'] = data.Date > adj_filter_date
            file_name = '{}.csv'.format(blueprint.output_file_name)
            self._clean_and_write_output(data, file_name)

        dh.close_connections()
        return

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _init_run(self, blueprint):
        if blueprint.constructor_type == 'universe':
            self._version_files = []

    def _init_rerun(self, output_dir_name, rerun_version):
        # Import meta data
        output_dir = os.path.join(self._prepped_data_dir,
                                  output_dir_name,
                                  rerun_version)
        path = os.path.join(output_dir, 'meta.json')
        meta = json.load(open(path, 'r'))
        blueprint = DataConstructorBlueprint(blueprint_json=meta['blueprint'])
        # Set instance variables for outputs
        self._version = rerun_version
        self._output_dir = output_dir
        if blueprint.constructor_type == 'universe':
            self._version_files = [x for x in os.listdir(output_dir)
                                   if x[-9:] == '_data.csv']
            self._version_files.sort()
            if 'market_index_data.csv' in self._version_files:
                self._version_files.remove('market_index_data.csv')
        print('[DataConstructor] - Restarting {}'.format(rerun_version))
        return blueprint

    def _check_file_completeness(self, blueprint):
        dh = DataHandlerSQL()
        all_dates = dh.get_all_dates()
        dh.close_connections()
        df = pd.DataFrame()
        # Iterate through backwards until first full file given
        # database dates
        date_iterator = self._make_date_iterator(blueprint)
        files_to_drop = []
        for file_name in reversed(self._version_files):
            # Get all unique dates from file
            path = os.path.join(self._output_dir, file_name)
            data = pd.read_csv(path, usecols=['Date'])
            data_dates = data.Date.unique()
            data_dates = convert_date_array(data_dates)
            # Match file name with date
            file_name_2 = file_name.split('_')[0]
            d = [d for d in date_iterator
                 if d[1].strftime('%Y%m%d') == file_name_2][0]
            # Get dates
            period_dates = all_dates[all_dates >= d[1]]
            period_dates = period_dates[period_dates <= d[2]]
            # Must have these test dates.
            if np.all(pd.Series(period_dates).isin(data_dates)):
                break
            else:
                files_to_drop.append(file_name)
        self._version_files = [x for x in self._version_files
                               if x not in files_to_drop]

    def _init_run_live(self, strategy_name):
        # Check if directories exist
        path = os.path.join(self._implementation_dir, strategy_name)
        if not os.path.isdir(path):
            os.mkdir(path)
        path = os.path.join(path, 'daily_raw_data')
        if not os.path.isdir(path):
            os.mkdir(path)
        self._output_dir = path

    # ~~~~~~ Output functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_output_directory(self, blueprint):
        # Get output directory name from blueprint. The name should
        # represent some class, or just general output
        output_dir = os.path.join(self._prepped_data_dir,
                                  blueprint.output_dir_name)
        # Make sure folder structure is in place
        if not os.path.isdir(self._prepped_data_dir):
            os.mkdir(self._prepped_data_dir)
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)
        archive_dir = os.path.join(output_dir, 'archive')
        if not os.path.isdir(archive_dir):
            os.mkdir(archive_dir)
        # Create new versioned directory
        versions = os.listdir(output_dir)
        versions = [x for x in versions if x[:3] == 'ver']
        if len(versions) == 0:
            self._output_dir = os.path.join(output_dir, 'version_0001')
            self._version = 'version_0001'
        else:
            version = 'version_{0:04d}'.format(
                int(max(versions).split('_')[1]) + 1)
            self._output_dir = os.path.join(output_dir,
                                            version)
            self._version = version
        os.mkdir(self._output_dir)

    def _write_archive_meta_data(self, blueprint):
        git_branch, git_commit = get_git_branch_commit()
        start_time = str(dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
        meta = {
            'start_time': start_time,
            'version': self._version,
            'git_branch': git_branch,
            'git_commit': git_commit,
            'blueprint': blueprint.to_json()
        }
        # Write meta to output directory
        path = os.path.join(self._output_dir, 'meta.json')
        with open(path, 'w') as outfile:
            json.dump(meta, outfile)
        # Write meta to archive
        path = os.path.join(self._prepped_data_dir,
                            blueprint.output_dir_name,
                            'archive', '{}.json'.format(self._version))
        with open(path, 'w') as outfile:
            json.dump(meta, outfile)

    def _update_meta_file(self, max_train_date, created_files):
        # Read and then rewrite
        path = os.path.join(self._output_dir, 'meta.json')
        meta = json.load(open(path, 'r'))
        meta['max_train_date'] = str(max_train_date.strftime('%Y-%m-%d'))
        meta['newly_created_files'] = created_files
        with open(path, 'w') as outfile:
            json.dump(meta, outfile)

    def _clean_and_write_output(self, data, file_name):
        if len(data) > 0:
            data = data.drop_duplicates()
            data.SecCode = data.SecCode.astype(int).astype(str)
            data = data.sort_values(['SecCode', 'Date'])
            data.to_csv(os.path.join(self._output_dir, file_name), index=False)

    # ~~~~~~ Iterator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_date_iterator(self, blueprint):
        # Extract
        date_params = blueprint.universe_date_parameters
        frequency = date_params['frequency']
        if 'quarter_frequency_month_offset' in date_params:
            q_offset = date_params['quarter_frequency_month_offset']
            assert q_offset in (0, 1, 2), 'Quarter offset can only be 0, 1, 2'
        else:
            q_offset = 0
        train_period_length = date_params['train_period_length']
        test_period_length = date_params['test_period_length']
        start_year = date_params['start_year']
        # Create
        if frequency == 'Q':
            periods = np.array([1, 4, 7, 10]) + q_offset
        elif frequency == 'M':
            periods = range(1, 13)
        all_periods = [dt.datetime(y, m, 1) for y, m in itertools.product(
            range(start_year-3, 2020), periods)]
        end_periods = [x - dt.timedelta(days=1) for x in all_periods]
        iterator = zip(all_periods[:-(train_period_length+test_period_length)],
                       all_periods[train_period_length:-test_period_length],
                       end_periods[train_period_length+test_period_length:])
        # Filter
        iterator = [x for x in iterator if x[1].year >= start_year]
        iterator = [x for x in iterator if x[1] <= dt.datetime.utcnow()]
        return iterator

    def _make_implementation_dates(self, blueprint):
        date_params = blueprint.universe_date_parameters
        frequency = date_params['frequency']
        if frequency == 'M':
            today = dt.date.today()
            filter_date = dt.date(today.year, today.month, 1)
            # Forward date
            end_date = filter_date + \
                dt.timedelta(days=(32 * date_params['test_period_length']))
            end_date = dt.date(end_date.year, end_date.month, 1)
            # Training period begin date - add one extra just in case
            start_date = filter_date - \
                dt.timedelta(days=(28 * date_params['train_period_length']))
            start_date = dt.date(start_date.year, start_date.month, 1)
            return start_date, filter_date, end_date
        else:
            raise NotImplementedError()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _check_parameters(self, blueprint):
        assert hasattr(blueprint, 'features')
        if blueprint.constructor_type == 'universe':
            params = blueprint.universe_filter_arguments
            assert 'filter' in params
            assert 'where' in params
            assert 'univ_size' in params
            params = blueprint.universe_date_parameters
            assert 'quarter_frequency_month_offset' in params
            assert 'train_period_length' in params
            assert 'test_period_length' in params
            assert 'frequency' in params
            assert 'start_year' in params

        if blueprint.constructor_type == 'universe_live':
            params = blueprint.universe_filter_arguments
            assert 'filter' in params
            assert 'where' in params
            assert 'univ_size' in params
            params = blueprint.universe_date_parameters
            assert 'quarter_frequency_month_offset' in params
            assert 'train_period_length' in params
            assert 'test_period_length' in params
            assert 'frequency' in params
            assert 'start_year' in params
            assert hasattr(blueprint, 'output_file_name')

        elif blueprint.constructor_type == 'etfs':
            assert 'start_date' in params
            assert 'end_date' in params
            assert 'tickers' in params
            assert 'output_file_name' in params

        elif blueprint.constructor_type == 'indexes':
            pass

        elif blueprint.constructor_type == 'seccodes':
            params = blueprint.seccodes_filter_arguments
            assert 'start_date' in params
            assert 'end_date' in params
            assert 'seccodes' in params
            assert 'output_file_name' in params

        if hasattr(blueprint, 'market_data_params'):
            params = blueprint.market_data_params
            assert 'features' in params
            assert 'seccodes' in params
        return True


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_data_version_name(strategy_name,
                          version_name,
                          prepped_data_dir=config.PREPPED_DATA_DIR,
                          cloud_flag=False):
    if cloud_flag:
        versions = _get_versions_cloud(strategy_name)
    else:
        versions = _get_versions(prepped_data_dir, strategy_name)
    try:
        return versions[int(version_name)]
    except:
        if version_name in versions.values():
            return version_name
        else:
            raise Exception('Version not found')


def _get_versions(prepped_data_dir, strategy_name):
    path = os.path.join(prepped_data_dir, strategy_name)

    dirs = [name for name in os.listdir(path)
            if os.path.isdir(os.path.join(path, name))]
    dirs = [x for x in dirs if x.find('version') >= 0]
    dirs.sort()
    return {i: d for i, d in enumerate(dirs)}


def _get_versions_cloud(strategy_name):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    all_files = [x.name for x in bucket.list_blobs()]
    all_files = [x for x in all_files if x.find('prepped_data') >= 0]
    all_files = [x for x in all_files if x.find('version') >= 0]
    all_files = [x for x in all_files if x.find(strategy_name + '/') >= 0]
    all_files = list(set([x.split('/')[2] for x in all_files]))
    all_files = [x for x in all_files if x.find('archive') == -1]
    all_files.sort()
    return {i: d for i, d in enumerate(all_files)}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def print_strategies(prepped_data_dir=config.PREPPED_DATA_DIR,
                     cloud_flag=False):
    if cloud_flag:
        strategies = _get_strategies_cloud()
    else:
        strategies = _get_strategies(prepped_data_dir)
    _print_line_underscore('Available Strategies with prepped data')
    for i, name in enumerate(strategies):
        print(' [{}]\t{}'.format(i, name))
    return


def get_strategy_name(strategy_name,
                      prepped_data_dir=config.PREPPED_DATA_DIR,
                      cloud_flag=False):
    if cloud_flag:
        strategies = _get_strategies_cloud()
    else:
        strategies = _get_strategies(prepped_data_dir)
    if strategy_name in strategies:
        return strategy_name
    try:
        strategy_index = int(strategy_name)
    except:
        return strategy_name
    for i, strategy in enumerate(strategies):
        if i == strategy_index:
            return strategy


def _get_strategies(prepped_data_dir):
    dirs = [name for name in os.listdir(prepped_data_dir)
            if os.path.isdir(os.path.join(prepped_data_dir, name))]
    dirs.sort()
    return dirs


def _get_strategies_cloud():
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    all_files = [x.name for x in bucket.list_blobs()
                 if x.name.find('prepped_data') > -1]
    dirs = list(set([x.split('/')[1] for x in all_files]))
    dirs.sort()
    return dirs


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def print_data_versions(strategy_name,
                        prepped_data_dir=config.PREPPED_DATA_DIR,
                        cloud_flag=False):
    if cloud_flag:
        stats = _get_strategy_version_stats_cloud(strategy_name)
    else:
        stats = _get_strategy_version_stats(strategy_name, prepped_data_dir)
    # Presentation
    _print_line_underscore('Available Verions for {}'.format(strategy_name))
    print('  Key\tVersion\t\t'
          'File Count\tMax Train Date\tDescription')
    keys = stats.keys()
    keys.sort()
    for key in keys:
        print('  [{}]\t{}\t{}\t\t{}\t\t{}'.format(
            key,
            stats[key]['version'],
            stats[key]['file_count'],
            stats[key]['max_train_date'],
            stats[key]['description']))
    print('\n')


def _get_strategy_version_stats(strategy_name, prepped_data_dir):
    versions = _get_versions(prepped_data_dir, strategy_name)
    # Get MinMax dates for files
    dir_stats = {}
    for key, version in versions.items():
        meta = _get_meta_data(prepped_data_dir, strategy_name, version)
        stats = _get_min_max_dates_counts(
            prepped_data_dir, strategy_name, version)
        max_train_date = meta['max_train_date'] if 'max_train_date' \
            in meta else None
        dir_stats[key] = {
            'version': version,
            'file_count': stats[2],
            'max_train_date': max_train_date,
            'description': meta['description']
        }
    return dir_stats


def _get_strategy_version_stats_cloud(strategy_name):
    versions = _get_versions_cloud(strategy_name)
    # Get MinMax dates for files
    dir_stats = {}
    for key, version in versions.items():
        meta = _get_meta_data_cloud(strategy_name, version)
        stats = _get_min_max_dates_counts_cloud(strategy_name, version)
        max_train_date = meta['max_train_date'] if 'max_train_date' \
            in meta else None
        dir_stats[key] = {
            'version': version,
            'file_count': stats[2],
            'max_train_date': max_train_date,
            'description': meta['description']
        }
    return dir_stats


def _get_meta_data(prepped_data_dir, strategy_name, version):
    path = os.path.join(prepped_data_dir, strategy_name,
                        version, 'meta.json')
    meta = json.load(open(path, 'r'))
    if 'blueprint' in meta:
        meta['description'] = meta['blueprint']['description']
    return meta


def _get_meta_data_cloud(strategy_name, version):
    path = os.path.join('prepped_data', strategy_name, version, 'meta.json')
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    blob = bucket.get_blob(path)
    try:
        meta = json.loads(blob.download_as_string())
        if 'blueprint' in meta:
            meta['description'] = meta['blueprint']['description']
        return meta
    except:
        meta = {'description': 'NO META DATA FOUND'}
        return meta


def _get_min_max_dates_counts(prepped_data_dir, strategy_name, version):
    files = os.listdir(os.path.join(prepped_data_dir,
                                    strategy_name, version))
    files = [f for f in files if f.find('_data.csv') > 1]
    if len(files):
        dates = [f.split('_')[0] for f in files]
        dates.sort()
        return dates[0], dates[-1], len(dates)
    else:
        return 'No Files', 'No Files', 0


def _get_min_max_dates_counts_cloud(strategy_name, version):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    all_files = [x.name for x in bucket.list_blobs()]
    all_files = [x for x in all_files if x.find('prepped_data') >= 0]
    all_files = [x for x in all_files if x.find(version) >= 0]
    all_files = [x for x in all_files if x.find(strategy_name) >= 0]
    all_files = [x for x in all_files if x.find('_data.csv') >= 0]
    all_files = list(set([x.split('/')[-1] for x in all_files]))
    all_files.sort()
    if len(all_files):
        dates = [f.split('_')[0] for f in all_files]
        dates.sort()
        return dates[0], dates[-1], len(dates)
    else:
        return 'No Files', 'No Files', 0


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _print_line_underscore(pstring):
    print('\n ' + pstring)
    print(' ' + '-' * len(pstring))
