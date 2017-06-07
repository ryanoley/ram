import os
import json
import itertools
import numpy as np
import datetime as dt

from gearbox import ProgBar

from ram import config

from ram.data.data_handler_sql import DataHandlerSQL
from ram.utils.documentation import get_git_branch_commit


class DataConstructor(object):

    def __init__(self, strategy):
        self.strategy_name = strategy.__class__.__name__
        self.features = strategy.get_features()
        self.date_parameters = strategy.get_date_parameters()
        self.filter_args = strategy.get_filter_args()
        self._prepped_data_dir = os.path.join(
            config.PREPPED_DATA_DIR, self.strategy_name)

    def run(self):
        self._check_parameters()
        self._make_output_directory()
        self._make_date_iterator()
        self._write_archive_meta_parameters()
        datahandler = DataHandlerSQL()
        for t1, t2, t3 in ProgBar(self._date_iterator):
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
            data.to_csv(os.path.join(self._output_dir, file_name), index=False)

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
        assert hasattr(self, 'date_parameters')
        assert hasattr(self, 'features')

    def _write_archive_meta_parameters(self):
        git_branch, git_commit = get_git_branch_commit()
        meta = {
            'frequency': self.date_parameters['frequency'],
            'filter_args': self.filter_args,
            'train_period_len': self.date_parameters['train_period_length'],
            'test_period_len': self.date_parameters['test_period_length'],
            'start_year': self.date_parameters['start_year'],
            'features': self.features,
            'start_time': str(dt.datetime.utcnow()),
            'strategy_name': self.strategy_name,
            'version': self.version,
            'git_branch': git_branch,
            'git_commit': git_commit
        }
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

def clean_directory():
    pass


def get_strategy_name(name):
    if name.isdigit():
        return _get_strategies()[int(name)]
    return name


def get_version_name(strategy, name):
    if name.isdigit():
        return _get_versions(strategy)[int(name)]
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


def print_strategies():
    dirs = _get_strategies()
    print('\n Available Strategies with prepped data')
    print(' --------------------------------------')
    for i, name in dirs.items():
        print('  [{}] '.format(i)+name)
    print('\n')


def print_strategy_versions(strategy):
    versions = _get_versions(strategy)
    pstring = '\n Available Verions for {}'.format(strategy)
    print(pstring)
    print(' ' + '-' * (len(pstring)-2))
    for i, v in versions.items():
        print('  [{}] '.format(i)+v)
    print('\n')


def print_strategy_meta(strategy, version):
    meta = _get_meta_data(strategy, version)
    print('\n Meta data for {} / {}'.format(strategy, version))
    print(' ' + '-' * (len(strategy) + len(version) + 17))
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
    print('   Frequency:\t' + str(meta['frequency']))
    print('\n')


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--clean', action='store_true',
        help='Clean all empty version directories')
    parser.add_argument(
        '-l', '--strategy', action='store_true',
        help='List all strategies')
    parser.add_argument(
        '-m', '--meta', nargs='+',
        help='Print meta data. If no version given, prints truncated view')
    args = parser.parse_args()

    if args.clean:
        clean_directory()
    elif args.strategy:
        print_strategies()
    elif args.meta:
        strategy = get_strategy_name(args.meta[0])
        if len(args.meta) > 1:
            version = get_version_name(strategy, args.meta[1])
            print_strategy_meta(strategy, version)
        else:
            print_strategy_versions(strategy)
