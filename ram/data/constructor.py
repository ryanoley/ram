import os
import json
import itertools
import numpy as np
import datetime as dt

from ram import config

from ram.data.dh_sql import DataHandlerSQL


class DataConstructor(object):

    def __init__(self, strategy_name):
        self._prepped_data_dir = os.path.join(
            config.PREPPED_DATA_DIR, strategy_name)

    def register_dates_parameters(self,
                                  frequency,
                                  train_period_length,
                                  start_year):
        self.frequency = frequency
        self.train_period_length = train_period_length
        self.start_year = start_year

    def register_features(self, features):
        self.features = features

    def run(self):
        self._check_parameters()
        self._make_output_directory()
        self._make_date_iterator()

    #def _get_data(self, start_date, filter_date, end_date, univ_size):
    #    # Adjust date by one day for filter date
    #    adj_filter_date = filter_date - dt.timedelta(days=1)
    #    filter_args = {
    #        'filter': 'AvgDolVol',
    #        'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
    #        'and Close_ between 15 and 1000',
    #        'univ_size': univ_size}
    #
    #    data = self.datahandler.get_filtered_univ_data(
    #        features=self.features,
    #        start_date=start_date,
    #        end_date=end_date,
    #        filter_date=adj_filter_date,
    #        filter_args=filter_args)
    #
    #    data = data.drop_duplicates()
    #    data.SecCode = data.SecCode.astype(str)
    #    data = data.sort_values(['SecCode', 'Date'])
    #
    #    return data

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_output_directory(self):
        if not os.path.isdir(self._prepped_data_dir):
            os.mkdir(self._prepped_data_dir)
        # Create new versioned directory
        versions = os.listdir(self._prepped_data_dir)
        if len(versions) == 0:
            os.mkdir(os.path.join(self._prepped_data_dir, 'version_001'))
        else:
            new_dir_name = 'version_{0:03d}'.format(
                int(max(versions).split('_')[1]) + 1)
            os.mkdir(os.path.join(self._prepped_data_dir, new_dir_name))

    def _make_date_iterator(self):
        if self.frequency == 'Q':
            periods = [1, 4, 7, 10]
        elif self.frequency == 'M':
            periods = range(1, 13)
        else:
            raise('Improper frequency input parameter: ', self.frequency)

        all_periods = [dt.datetime(y, m, 1) for y, m in itertools.product(
            range(self.start_year-1, 2020), periods)]
        # Filter
        ind = np.where(np.array(all_periods) > dt.datetime.utcnow())[0][0] + 1
        all_periods = all_periods[:ind]

        iterator = zip(all_periods[:-(self.train_period_length+1)],
                       all_periods[self.train_period_length:-1],
                       all_periods[self.train_period_length+1:])

        self._date_iterator = iterator

    def _check_parameters(self):
        assert hasattr(self, 'frequency')
        assert hasattr(self, 'train_period_length')
        assert hasattr(self, 'start_year')
        assert hasattr(self, 'features')
