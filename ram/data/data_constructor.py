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
from ram.utils.documentation import prompt_for_description


class DataConstructor(object):

    def __init__(self, prepped_data_dir=config.PREPPED_DATA_DIR):
        self._prepped_data_dir = prepped_data_dir

    # ~~~~~~ Interface ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run(self, blueprint, description=None):
        self._check_parameters(blueprint)
        self._init_run(blueprint)
        self._make_output_directory(blueprint)
        self._write_archive_meta_data(blueprint, description)
        self._make_data(blueprint)

    def rerun(self, output_dir_name, rerun_version):
        blueprint = self._init_rerun(output_dir_name, rerun_version)
        self._check_file_completeness(blueprint)
        self._make_data(blueprint)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_data(self, blueprint):

        dh = DataHandlerSQL()

        if blueprint.constructor_type == 'etfs':
            start_date = blueprint.etfs_filter_arguments['start_date']
            data = dh.get_etf_data(
                tickers=blueprint.etfs_filter_arguments['tickers'],
                features=blueprint.features,
                start_date=start_date,
                end_date=blueprint.etfs_filter_arguments['end_date'])
            file_name = '{}_data.csv'.format(start_date)
            self._clean_and_write_output(data, file_name)

        elif blueprint.constructor_type == 'seccodes':
            start_date = blueprint.seccodes_filter_arguments['start_date']
            data = dh.get_seccode_data(
                seccodes=blueprint.seccodes_filter_arguments['seccodes'],
                features=blueprint.features,
                start_date=start_date,
                end_date=blueprint.seccodes_filter_arguments['end_date'])
            file_name = '{}_data.csv'.format(start_date)
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
            self._update_meta_file(data.Date.max(), created_files)

        # Market data
        if hasattr(blueprint, 'market_data_params'):
            params = blueprint.market_data_params
            data = dh.get_index_data(
                seccodes=params['seccodes'],
                features=params['features'],
                start_date='1990-01-01',
                end_date='2050-04-01')
            self._clean_and_write_output(data, 'market_index_data.csv')

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
            data = pd.read_csv(path)
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
            os.mkdir(os.path.join(output_dir, 'archive'))
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

    def _write_archive_meta_data(self,
                                 blueprint,
                                 description=True):
        description =  description if description else prompt_for_description()
        git_branch, git_commit = get_git_branch_commit()
        start_time = str(dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
        meta = {
            'start_time': start_time,
            'version': self._version,
            'git_branch': git_branch,
            'git_commit': git_commit,
            'description': description,
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

    def _update_meta_file(self, max_date, created_files):
        # Read and then rewrite
        path = os.path.join(self._output_dir, 'meta.json')
        meta = json.load(open(path, 'r'))
        meta['max_date'] = str(max_date.strftime('%Y-%m-%d'))
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
        # Filter
        ind = np.where(np.array(all_periods) > dt.datetime.utcnow())[0][0] + 1
        all_periods = all_periods[:ind]
        end_periods = [x - dt.timedelta(days=1) for x in all_periods]
        iterator = zip(all_periods[:-(train_period_length+test_period_length)],
                       all_periods[train_period_length:-test_period_length],
                       end_periods[train_period_length+test_period_length:])
        iterator = [x for x in iterator if x[1].year >= start_year]
        return iterator

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

        elif blueprint.constructor_type == 'etfs':
            pass

        elif blueprint.constructor_type == 'indexes':
            pass

        elif blueprint.constructor_type == 'seccodes':
            pass

        if hasattr(blueprint, 'market_data_params'):
            params = blueprint.market_data_params
            assert 'features' in params
            assert 'seccodes' in params
        return True

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clean_directory(strategy, version, cloud_flag):
    if cloud_flag:
        print('This functionality not setup. Must delete from GCP Dashboard.')
        return
    dir_path = os.path.join(config.PREPPED_DATA_DIR, strategy, version)
    print('\n Are you sure you want to delete the following path:')
    print('   ' + dir_path)
    print('\n Type `1234` if you are certain: ')
    user_input = raw_input()
    if user_input == '1234':
        print('\nDeleting\n')
        shutil.rmtree(dir_path)
    else:
        print('\nNo versions deleted\n')


def get_strategy_name(name):
    try:
        return _get_strategies()[int(name)]
    except:
        return name


def get_version_name(strategy, name, cloud_flag=False):
    if cloud_flag:
        try:
            return _get_versions_cloud(strategy)[int(name)]
        except:
            return name
    else:
        try:
            return _get_versions(strategy)[int(name)]
        except:
            return name


def _get_directories(path):
    return [name for name in os.listdir(path)
            if os.path.isdir(os.path.join(path, name))]


def _get_strategies():
    dirs = _get_directories(config.PREPPED_DATA_DIR)
    dirs.sort()
    return {i: d for i, d in enumerate(dirs)}


def _get_versions(strategy):
    dirs = _get_directories(os.path.join(config.PREPPED_DATA_DIR, strategy))
    dirs.sort()
    dirs = [x for x in dirs if x.find('version') >= 0]
    return {i: d for i, d in enumerate(dirs)}


def _get_versions_cloud(strategy):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    all_files = [x.name for x in bucket.list_blobs()]
    all_files = [x for x in all_files if x.find('prepped_data') >= 0]
    all_files = [x for x in all_files if x.find('version') >= 0]
    all_files = [x for x in all_files if x.find(strategy) >= 0]
    all_files = list(set([x.split('/')[2] for x in all_files]))
    all_files = [x for x in all_files if x.find('archive') == -1]
    all_files.sort()
    return {i: d for i, d in enumerate(all_files)}


def _get_meta_data(strategy, version):
    path = os.path.join(config.PREPPED_DATA_DIR, strategy,
                        version, 'meta.json')
    try:
        with open(path) as data_file:
            meta = json.load(data_file)
        if 'description' not in meta:
            meta['description'] = None
        return meta
    except:
        return {}


def _get_meta_data_cloud(strategy, version):
    path = os.path.join('prepped_data', strategy, version, 'meta.json')
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    blob = bucket.get_blob(path)
    try:
        meta = json.loads(blob.download_as_string())
    except Exception as e:
        meta = {
            'description': 'No meta file found',
            'start_time': 'No meta file found',
        }
    if 'description' not in meta:
        meta['description'] = None
    return meta


def _get_min_max_dates_counts(strategy, version):
    files = os.listdir(os.path.join(config.PREPPED_DATA_DIR,
                                    strategy, version))
    files = [f for f in files if f.find('_data.csv') > 1]
    if len(files):
        dates = [f.split('_')[0] for f in files]
        dates.sort()
        return dates[0], dates[-1], len(dates)
    else:
        return 'No Files', 'No Files', 0


def _get_min_max_dates_counts_cloud(strategy, version):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
    all_files = [x.name for x in bucket.list_blobs()]
    all_files = [x for x in all_files if x.find('prepped_data') >= 0]
    all_files = [x for x in all_files if x.find(version) >= 0]
    all_files = [x for x in all_files if x.find(strategy) >= 0]
    all_files = [x for x in all_files if x.find('_data.csv') >= 0]
    all_files = list(set([x.split('/')[-1] for x in all_files]))
    all_files.sort()
    if len(all_files):
        dates = [f.split('_')[0] for f in all_files]
        dates.sort()
        return dates[0], dates[-1], len(dates)
    else:
        return 'No Files', 'No Files', 0


def print_strategies():
    dirs = _get_strategies()
    _print_line_underscore('Available Strategies with prepped data')
    for i, name in dirs.items():
        print('  [{}] '.format(i)+name)
    print('\n')


def _print_line_underscore(pstring):
    print('\n ' + pstring)
    print(' ' + '-' * len(pstring))


def print_strategy_data_versions(strategy, cloud_flag=False):
    stats = _get_strategy_version_stats(strategy, cloud_flag)
    _print_line_underscore('Available Verions for {}'.format(strategy))
    print('  Key\tVersion\t\t'
          'File Count\tMax Data Date\tDescription')
    keys = stats.keys()
    keys.sort()
    for key in keys:
        print('  [{}]\t{}\t{}\t\t{}\t\t{}'.format(
            key,
            stats[key]['version'],
            stats[key]['file_count'],
            stats[key]['max_date'],
            stats[key]['description']))
    print('\n')


def _get_strategy_version_stats(strategy, cloud_flag=False):

    if cloud_flag:
        versions = _get_versions_cloud(strategy)
    else:
        versions = _get_versions(strategy)

    # Get MinMax dates for files
    dir_stats = {}
    for key, version in versions.items():

        if cloud_flag:
            meta = _get_meta_data_cloud(strategy, version)
            stats = _get_min_max_dates_counts_cloud(strategy, version)
        else:
            meta = _get_meta_data(strategy, version)
            stats = _get_min_max_dates_counts(strategy, version)

        max_date = meta['max_date'][:10] if 'max_date' in meta else None
        dir_stats[key] = {
            'version': version,
            'file_count': stats[2],
            'max_date': max_date,
            'description': meta['description']
        }
    return dir_stats


def print_strategy_meta(strategy, version, cloud_flag):
    if cloud_flag:
        version = get_version_name_cloud(strategy, version)
        meta = _get_meta_data_cloud(strategy, version)
    else:
        version = get_version_name(strategy, version)
        meta = _get_meta_data(strategy, version)
    # Print outputs
    _print_line_underscore('Meta data for {} / {}'.format(strategy, version))
    print('   Git Branch:\t' + str(meta['git_branch']))
    print('   Features:\t' + meta['features'][0])
    for feature in meta['features'][1:]:
        print('\t\t{}'.format(feature))
    print('\n')
    print('   Filter Arguments: ')
    print('\tUniverse Size:\t' + str(meta['filter_args_univ']['univ_size']))
    print('\tFilter:\t\t' + meta['filter_args_univ']['filter'])
    print('\tWhere:\t\t' + meta['filter_args_univ']['where'])
    print('\n')
    meta_t = meta['date_parameters_univ']
    print('   Start Year:\t\t' + str(meta_t['start_year']))
    print('   Train Period Length:\t' + str(meta_t['train_period_length']))
    print('   Test Period Length:\t' + str(meta_t['test_period_length']))
    print('   Frequency:\t\t' + str(meta_t['frequency']))
    print('\n')
