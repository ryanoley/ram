import os
import json
import shutil
import itertools
import numpy as np
from tqdm import tqdm
import datetime as dt
from dateutil import parser as dparser

from ram import config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.utils.documentation import get_git_branch_commit


class DataConstructor(object):

    def __init__(self, strategy):
        self.strategy_name = strategy.__class__.__name__
        self.constructor_type = strategy.get_constructor_type()
        self.features = strategy.get_features()
        self._prepped_data_dir = os.path.join(
            config.PREPPED_DATA_DIR, self.strategy_name)
        if self.constructor_type in ['etfs', 'ids']:
            self.filter_args = strategy.get_ids_filter_args()
        else:
            self.date_parameters = strategy.get_univ_date_parameters()
            self.filter_args = strategy.get_univ_filter_args()

    def run(self):
        self._check_parameters()
        self._make_output_directory()
        self._write_archive_meta_parameters()
        datahandler = DataHandlerSQL()
        if self.constructor_type in ['etfs', 'ids']:
            if self.constructor_type == 'etfs':
                data = datahandler.get_etf_data(
                    self.filter_args['ids'],
                    self.features,
                    self.filter_args['start_date'],
                    self.filter_args['end_date'])
            else:
                data = datahandler.get_id_data(
                    self.filter_args['ids'],
                    self.features,
                    self.filter_args['start_date'],
                    self.filter_args['end_date'])
            data = data.drop_duplicates()
            data.SecCode = data.SecCode.astype(int).astype(str)
            data = data.sort_values(['SecCode', 'Date'])
            start_date = dparser.parse(self.filter_args['start_date'])
            file_name = '{}_data.csv'.format(start_date.strftime('%Y%m%d'))
            data.to_csv(os.path.join(self._output_dir, file_name), index=False)
        else:
            self._make_date_iterator()
            for t1, t2, t3 in tqdm(self._date_iterator):
                adj_filter_date = t2 - dt.timedelta(days=1)
                data = datahandler.get_filtered_univ_data(
                    features=self.features,
                    start_date=t1,
                    end_date=t3,
                    filter_date=adj_filter_date,
                    filter_args=self.filter_args)
                data = data.drop_duplicates()
                data.SecCode = data.SecCode.astype(int).astype(str)
                data = data.sort_values(['SecCode', 'Date'])
                # Add TestFlag
                data['TestFlag'] = data.Date > adj_filter_date
                file_name = '{}_data.csv'.format(t2.strftime('%Y%m%d'))
                data.to_csv(os.path.join(self._output_dir, file_name),
                            index=False)            


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
            print('[DataConstructor] : Making {}'.format(new_dir_name))
        os.mkdir(self._output_dir)

    def _make_date_iterator(self):
        # Extract parameters
        frequency = self.date_parameters['frequency']
        train_period_length = self.date_parameters['train_period_length']
        test_period_length = self.date_parameters['test_period_length']
        start_year = self.date_parameters['start_year']
        # Create
        if frequency == 'Q':
            periods = [1, 4, 7, 10]
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
        assert hasattr(self, 'constructor_type')
        assert hasattr(self, 'features')
        assert hasattr(self, 'filter_args')
        if self.constructor_type == 'universe':  
            assert hasattr(self, 'date_parameters')
            assert set(['filter', 'univ_size']).issubset(
                self.filter_args.keys())
        if self.constructor_type in ['etfs','ids']:  
            assert set(['ids', 'start_date', 'end_date']).issubset(
                self.filter_args.keys())

    def _write_archive_meta_parameters(self):
        git_branch, git_commit = get_git_branch_commit()
        meta = {
            'filter_args': self.filter_args,
            'features': self.features,
            'start_time': str(dt.datetime.utcnow()),
            'strategy_name': self.strategy_name,
            'version': self.version,
            'git_branch': git_branch,
            'git_commit': git_commit
        }
        if self.constructor_type == 'universe':
            meta.update({
                'frequency': self.date_parameters['frequency'],
                'train_period_len': self.date_parameters['train_period_length'],
                'test_period_len': self.date_parameters['test_period_length'],
                'start_year': self.date_parameters['start_year']
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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clean_directory(strategy, version):
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


def _get_meta_data(strategy, version):
    path = os.path.join(config.PREPPED_DATA_DIR, strategy,
                        version, 'meta.json')
    with open(path) as data_file:
        meta = json.load(data_file)
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


def print_strategies():
    dirs = _get_strategies()
    _print_line_underscore('Available Strategies with prepped data')
    for i, name in dirs.items():
        print('  [{}] '.format(i)+name)
    print('\n')


def _print_line_underscore(pstring):
    print('\n ' + pstring)
    print(' ' + '-' * len(pstring))


def print_strategy_versions(strategy):
    stats = _get_strategy_version_stats(strategy)
    _print_line_underscore('Available Verions for {}'.format(strategy))
    print('  Key\tVersion\t\tStart Date\tEnd Date\t'
          'File Count\tDir Creation Date')
    keys = stats.keys()
    keys.sort()
    for key in keys:
        print('  [{}]\t{}\t{}\t{}\t{}\t\t{}'.format(
            key,
            stats[key]['version'],
            stats[key]['min_date'],
            stats[key]['max_date'],
            stats[key]['file_count'],
            stats[key]['create_date']))
    print('\n')


def _get_strategy_version_stats(strategy):
    versions = _get_versions(strategy)
    # Get MinMax dates for files
    dir_stats = {}
    for key, version in versions.items():
        meta = _get_meta_data(strategy, version)
        stats = _get_min_max_dates_counts(strategy, version)
        dir_stats[key] = {
            'version': version,
            'min_date': stats[0],
            'max_date': stats[1],
            'file_count': stats[2],
            'create_date': meta['start_time'][:10]
        }
    return dir_stats


def print_strategy_meta(strategy, version):
    meta = _get_meta_data(strategy, version)
    _print_line_underscore('Meta data for {} / {}'.format(strategy, version))
    print('   Git Branch:\t' + str(meta['git_branch']))
    print('   Features:\t' + meta['features'][0])
    for feature in meta['features'][1:]:
        print('\t\t{}'.format(feature))
    print('\n')
    print('   Filter Arguments: ')
    print('\tUniverse Size:\t' + str(meta['filter_args']['univ_size']))
    print('\tFilter:\t\t' + meta['filter_args']['filter'])
    print('\tWhere:\t\t' + meta['filter_args']['where'])
    print('\n')
    print('   Start Year:\t\t' + str(meta['start_year']))
    print('   Train Period Length:\t' + str(meta['train_period_len']))
    print('   Test Period Length:\t' + str(meta['test_period_len']))
    print('   Frequency:\t\t' + str(meta['frequency']))
    print('\n')


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-ls', '--list_strategies', action='store_true',
        help='List all strategies')
    parser.add_argument(
        '-lv', '--list_versions', type=str,
        help='List all versions of prepped data for a strategy')
    parser.add_argument(
        '-pm', '--print_meta', type=str, nargs=2,
        help='Print meta data. Takes two arguments for Strategy and Version')
    parser.add_argument(
        '-cv', '--clean_version', type=str, nargs=2,
        help='Delete version. Takes two arguments for Strategy and Version')

    args = parser.parse_args()

    if args.list_strategies:
        print_strategies()
    elif args.list_versions:
        strategy = get_strategy_name(args.list_versions)
        print_strategy_versions(strategy)
    elif args.print_meta:
        strategy = get_strategy_name(args.print_meta[0])
        version = get_version_name(strategy, args.print_meta[1])
        print_strategy_meta(strategy, version)
    elif args.clean_version:
        strategy = get_strategy_name(args.clean_version[0])
        version = get_version_name(strategy, args.clean_version[1])
        clean_directory(strategy, version)
