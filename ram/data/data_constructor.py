import os
import json
import itertools
import numpy as np
import datetime as dt

from gearbox import ProgBar

from ram import config

from ram.data.data_handler_sql import DataHandlerSQL


class DataConstructor(object):

    # DEFAULTS
    filter_args = {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': 500}

    def __init__(self, strategy_name):
        self._prepped_data_dir = os.path.join(
            config.PREPPED_DATA_DIR, strategy_name)
        self.strategy_name = strategy_name

    def register_universe_size(self, univ_size):
        self.filter_args['univ_size'] = univ_size

    def register_dates_parameters(self,
                                  frequency,
                                  train_period_length,
                                  start_year):
        """
        Parameters
        ----------
        frequency : str
            'Q' for quarter and 'M' for monthly
        train_period_len : int
            Number of periods (quarters or months) to provide
            training data for. Training and test data are flagged as a
            column in the data
        start_year : int
            Year
        """
        assert frequency.upper() in ['Q', 'M']
        self.frequency = frequency.upper()
        self.train_period_length = train_period_length
        self.start_year = start_year

    def register_features(self, features):
        self.features = features

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
            data.SecCode = data.SecCode.astype(str)
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
        os.mkdir(self._output_dir)

    def _make_date_iterator(self):
        if self.frequency == 'Q':
            periods = [1, 4, 7, 10]
        elif self.frequency == 'M':
            periods = range(1, 13)

        all_periods = [dt.datetime(y, m, 1) for y, m in itertools.product(
            range(self.start_year-1, 2020), periods)]
        # Filter
        ind = np.where(np.array(all_periods) > dt.datetime.utcnow())[0][0] + 1
        all_periods = all_periods[:ind]
        end_periods = [x - dt.timedelta(days=1) for x in all_periods]

        iterator = zip(all_periods[:-(self.train_period_length+1)],
                       all_periods[self.train_period_length:-1],
                       end_periods[self.train_period_length+1:])

        self._date_iterator = iterator

    def _check_parameters(self):
        assert hasattr(self, 'frequency')
        assert hasattr(self, 'train_period_length')
        assert hasattr(self, 'start_year')
        assert hasattr(self, 'features')

    def _write_archive_meta_parameters(self):
        meta = {
            'frequency': self.frequency,
            'train_period_len': self.train_period_length,
            'start_year': self.start_year,
            'features': self.features,
            'start_time': str(dt.datetime.utcnow()),
            'strategy_name': self.strategy_name,
            'version': self.version
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


def clean_directory():
    pass


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--clean', action='store_true',
        help='Clean all empty version directories')
    args = parser.parse_args()

    if args.clean:
        clean_directory()
