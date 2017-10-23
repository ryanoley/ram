import os
import json
import shutil
import itertools
import numpy as np
from tqdm import tqdm
import datetime as dt
from dateutil import parser as dparser

from google.cloud import storage

from ram import config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.utils.documentation import get_git_branch_commit
from ram.utils.documentation import prompt_for_description


class DataConstructor(object):

    def __init__(self,
                 strategy,
                 prepped_data_dir=config.PREPPED_DATA_DIR):
        self.strategy = strategy
        self.strategy_name = strategy.__class__.__name__
        self._prepped_data_dir = os.path.join(
            prepped_data_dir, self.strategy_name)

    def _init_new_run(self):
        self.constructor_type = self.strategy.get_constructor_type()
        self.features = self.strategy.get_features()
        if self.constructor_type in ['etfs', 'ids']:
            self.filter_args_ids = self.strategy.get_ids_filter_args()
        else:
            self.filter_args_univ = self.strategy.get_univ_filter_args()
            self.date_parameters_univ = \
                self.strategy.get_univ_date_parameters()
            self.version_files = []

    def _init_rerun_run(self, version):
        self.version = version
        self._output_dir = os.path.join(self._prepped_data_dir, version)
        ddir = os.path.join(self._prepped_data_dir, version)
        path = os.path.join(ddir, 'meta.json')
        with open(path) as data_file:
            meta = json.load(data_file)
        data_file.close()
        # Extract data to instance variables
        self.constructor_type = meta['constructor_type']
        self.features = meta['features']
        if self.constructor_type == 'universe':
            self.filter_args_univ = meta['filter_args_univ']
            self.date_parameters_univ = meta['date_parameters_univ']
            self.version_files = [
                x for x in os.listdir(ddir) if x[-3:] == 'csv']
        elif self.constructor_type in ['etfs', 'ids']:
            self.filter_args_ids = meta['filter_args_ids']

    def run(self, rerun_version=None, prompt_description=True):
        if rerun_version:
            self._init_rerun_run(rerun_version)
        else:
            self._init_new_run()
            self._make_output_directory()
            self._write_archive_meta_parameters(prompt_description)

        self._check_parameters()

        datahandler = DataHandlerSQL()

        if self.constructor_type == 'etfs':
            data = datahandler.get_etf_data(
                self.filter_args_ids['ids'],
                self.features,
                self.filter_args_ids['start_date'],
                self.filter_args_ids['end_date'])
            start_date = dparser.parse(self.filter_args_ids['start_date'])
            file_name = '{}_data.csv'.format(start_date.strftime('%Y%m%d'))
            self._clean_write_output(data, file_name)

        elif self.constructor_type == 'ids':
            data = datahandler.get_id_data(
                self.filter_args_ids['ids'],
                self.features,
                self.filter_args_ids['start_date'],
                self.filter_args_ids['end_date'])
            start_date = dparser.parse(self.filter_args_ids['start_date'])
            file_name = '{}_data.csv'.format(start_date.strftime('%Y%m%d'))
            self._clean_write_output(data, file_name)

        else:
            self._make_date_iterator()
            for t1, t2, t3 in tqdm(self._date_iterator):
                # Check if file already exists in output directory
                file_name = '{}_data.csv'.format(t2.strftime('%Y%m%d'))
                if file_name in self.version_files:
                    continue
                # Otherwise pull and process data
                adj_filter_date = t2 - dt.timedelta(days=1)
                data = datahandler.get_filtered_univ_data(
                    features=self.features,
                    start_date=t1,
                    end_date=t3,
                    filter_date=adj_filter_date,
                    filter_args=self.filter_args_univ)
                data['TestFlag'] = data.Date > adj_filter_date
                self._clean_write_output(data, file_name)

    def run_index_data(self, version_directory):
        args = self.strategy.get_market_index_data_arguments()
        dh = DataHandlerSQL()
        data = dh.get_index_data(seccodes=args['seccodes'],
                                 features=args['features'],
                                 start_date='1990-01-01',
                                 end_date='2050-04-01')
        self._output_dir = os.path.join(
            self._prepped_data_dir, version_directory)
        self._clean_write_output(data, 'market_index_data.csv')

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_output_directory(self):
        if not os.path.isdir(self._prepped_data_dir):
            os.mkdir(self._prepped_data_dir)
            os.mkdir(os.path.join(self._prepped_data_dir, 'archive'))
        # Create new versioned directory
        versions = os.listdir(self._prepped_data_dir)
        versions = [x for x in versions if x[:3] == 'ver']
        if len(versions) == 0:
            self._output_dir = os.path.join(self._prepped_data_dir,
                                            'version_0001')
            self.version = 'version_0001'
        else:
            new_dir_name = 'version_{0:04d}'.format(
                int(max(versions).split('_')[1]) + 1)
            self._output_dir = os.path.join(self._prepped_data_dir,
                                            new_dir_name)
            self.version = new_dir_name
        print('[DataConstructor] : Making {}'.format(self.version))
        os.mkdir(self._output_dir)

    def _make_date_iterator(self):
        # Extract parameters
        frequency = self.date_parameters_univ['frequency']
        if 'quarter_frequency_month_offset' in self.date_parameters_univ:
            q_offset = self.date_parameters_univ[
                'quarter_frequency_month_offset']
            assert q_offset in (0, 1, 2), 'Quarter offset can only be 0, 1, 2'
        else:
            q_offset = 0
        train_period_length = self.date_parameters_univ['train_period_length']
        test_period_length = self.date_parameters_univ['test_period_length']
        start_year = self.date_parameters_univ['start_year']
        # Create
        if frequency == 'Q':
            periods = np.array([1, 4, 7, 10]) + q_offset
        elif frequency == 'M':
            periods = range(1, 13)
        all_periods = [dt.datetime(y, m, 1) for y, m in itertools.product(
            range(start_year-1, 2020), periods)]
        # Filter
        ind = np.where(np.array(all_periods) > dt.datetime.utcnow())[0][0] + 1
        all_periods = all_periods[:ind]
        end_periods = [x - dt.timedelta(days=1) for x in all_periods]
        iterator = zip(all_periods[:-(train_period_length+test_period_length)],
                       all_periods[train_period_length:-test_period_length],
                       end_periods[train_period_length+test_period_length:])
        self._date_iterator = iterator

    def _check_parameters(self):
        """
        Checks that the correct parameters exist given the implementation
        of Strategy, checking specifically for `universe` or `ids`
        implementation arguments.
        """
        assert hasattr(self, 'constructor_type')
        assert hasattr(self, 'features')

        if self.constructor_type == 'universe':
            assert hasattr(self, 'date_parameters_univ')
            assert hasattr(self, 'filter_args_univ')
            assert set(['filter', 'univ_size']).issubset(
                self.filter_args_univ.keys())

        elif self.constructor_type in ['etfs', 'ids']:
            assert hasattr(self, 'filter_args_ids')
            assert set(['ids', 'start_date', 'end_date']).issubset(
                self.filter_args_ids.keys())

    def _write_archive_meta_parameters(self, description_prompt=True):
        # Flag for testing purposes
        description = prompt_for_description() if description_prompt else None
        git_branch, git_commit = get_git_branch_commit()

        meta = {
            'features': self.features,
            'start_time': str(dt.datetime.utcnow()),
            'strategy_name': self.strategy_name,
            'version': self.version,
            'git_branch': git_branch,
            'git_commit': git_commit,
            'description': description,
            'constructor_type': self.constructor_type
        }

        if self.constructor_type == 'universe':
            meta.update({
                'date_parameters_univ': self.date_parameters_univ,
                'filter_args_univ': self.filter_args_univ
            })
        elif self.constructor_type in ['ids', 'etfs']:
            meta.update({
                'filter_args_ids': self.filter_args_ids
            })

        # Write meta to output directory
        path = os.path.join(self._output_dir, 'meta.json')
        with open(path, 'w') as outfile:
            json.dump(meta, outfile)
        outfile.close()
        # Write meta to archive
        path = os.path.join(self._prepped_data_dir, 'archive',
                            '{}_{}.json'.format(self.strategy_name,
                                                self.version))
        with open(path, 'w') as outfile:
            json.dump(meta, outfile)
        outfile.close()

    def _clean_write_output(self, data, file_name):
        if len(data) > 0:
            data = data.drop_duplicates()
            data.SecCode = data.SecCode.astype(int).astype(str)
            data = data.sort_values(['SecCode', 'Date'])
            data.to_csv(os.path.join(self._output_dir, file_name), index=False)


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


def get_version_name(strategy, name):
    try:
        return _get_versions(strategy)[int(name)]
    except:
        return name


def get_version_name_cloud(strategy, name):
    try:
        return _get_versions_cloud(strategy)[int(name)]
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


def print_strategy_versions(strategy, cloud_flag=False):
    stats = _get_strategy_version_stats(strategy, cloud_flag)
    _print_line_underscore('Available Verions for {}'.format(strategy))
    print('  Key\tVersion\t\t'
          'File Count\tDir Creation Date\tDescription')
    keys = stats.keys()
    keys.sort()
    for key in keys:
        print('  [{}]\t{}\t{}\t\t{}\t\t{}'.format(
            key,
            stats[key]['version'],
            stats[key]['file_count'],
            stats[key]['create_date'],
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

        dir_stats[key] = {
            'version': version,
            'file_count': stats[2],
            'create_date': meta['start_time'][:10],
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
