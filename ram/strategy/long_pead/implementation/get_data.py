import os
import shutil
import numpy as np
import pandas as pd
import datetime as dt

from ram.data.data_handler_sql import DataHandlerSQL
from ram import config as app_config
from ram.strategy.long_pead.implementation import config


class ImplementationMorningDataPull(object):

    def __init__(self):
        # Check directory
        self.raw_data_path = self._get_data_path()

    def morning_data_pull(self, write_flag=False):
        datahandler = DataHandlerSQL()
        ids = config.ids['sector20']
        features = config.data_pull_features
        end_date = dt.datetime.today().date() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=400)
        data = datahandler.get_id_data(ids, features, start_date, end_date)
        if not write_flag:
            return
        path = os.path.join(self.raw_data_path, 'raw_data.csv')
        if os.path.isfile(path):
            today = dt.datetime.utcnow().strftime('%Y%m%d')
            path2 = os.path.join(self.raw_data_path, 'archive',
                                 'raw_data_{}.csv'.format(today))
            shutil.move(path, path2)
        data.to_csv(path, index=False)

    def _get_data_path(self):
        path1 = os.path.join(app_config.IMPLEMENTATION_DATA_DIR,
                             'LongPeadStrategy')
        if not os.path.isdir(path1):
            os.mkdir(path1)
        path2 = os.path.join(path1, 'raw_data')
        if not os.path.isdir(path2):
            os.mkdir(path2)
        path3 = os.path.join(path2, 'archive')
        if not os.path.isdir(path3):
            os.mkdir(path3)
        return path2


if __name__ == '__main__':
    
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--morning_pull', action='store_true')
    args = parser.parse_args()

    if args.morning_pull:
        ImplementationMorningDataPull().morning_data_pull(True)
