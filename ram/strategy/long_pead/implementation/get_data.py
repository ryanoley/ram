import os
import json
import shutil
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm

from ram.config import IMPLEMENTATION_DATA_DIR
from ram.data.data_handler_sql import DataHandlerSQL
from ram.strategy.long_pead.implementation import config


class ImplementationTrainingDataPull(object):

    def __init__(self, imp_data_dir=IMPLEMENTATION_DATA_DIR):
        self.raw_data_path = os.path.join(
            _get_data_path(imp_data_dir), 'training')
        self.dh = DataHandlerSQL()

    def start(self):
        # Query all ids
        ids = self.get_ids()
        start_date, end_date = self.get_data_query_dates()
        for sector in config.sectors:
            sector_ids = ids[sector]
            data = dh.get_id_data(
                    ids=sector_ids,
                    features=config.features,
                    start_date=start_date,
                    end_date=end_date)
            self.write_sector_data(data, sector)

    def get_ids(self):
        dates = self.get_ids_query_dates()
        sector_ids = {}
        sector_ids_training = {}
        for sector in config.sectors:
            filter_args = {
                'filter': 'AvgDolVol',
                'where': 'MarketCap >= 200 and Close_ between 5 and 500' +
                         'and GSECTOR = {}'.format(sector),
                'univ_size': 800}
            sector_ids[sector] = []
            sector_ids_training[sector] = {}
            for d in dates:
                start_date = d - dt.timedelta(days=2)
                univ = self.dh.get_filtered_univ_data(
                    features=['AdjClose'],
                    start_date=start_date,
                    end_date=d,
                    filter_date=d,
                    filter_args=filter_args)
                ids = univ.SecCode.unique()
                sector_ids[sector].append(ids)
                sector_ids_training[sector][d.strftime('%Y%m%d')] = ids.tolist()
            sector_ids[sector] = \
                np.unique(np.concatenate(sector_ids[sector])).tolist()
        # Write training ids to file
        self.write_sector_id_data(sector_ids_training)
        return sector_ids

    def get_ids_query_dates(self):
        today = dt.datetime.today().date()
        mdates = [dt.date(y, m, 1) for y in range(2015, today.year + 1)
                  for m in range(1, 13)]
        mdates = [d for d in mdates if d < today]
        mdates = mdates[-13:]
        mdates = [d - dt.timedelta(days=1) for d in mdates]
        return mdates

    def get_data_query_dates(self):
        today = dt.datetime.today().date()
        start_date = dt.date(today.year-2, today.month, 1)
        end_date = dt.date(today.year, today.month, 1)
        return start_date, end_date

    def write_sector_data(self, data, sector):
        output_path = os.path.join(
            self.raw_data_path, 'raw_sector_{}.csv'.format(sector))
        # Check if current file needs to be tagged and moved to archive
        if os.path.isfile(output_path):
            datestamp = dt.datetime.now().strftime('%Y%m%d')
            new_file_name = 'raw_sector_{}'.format(sector)
            new_file_name += '_moved_{}.csv'.format(datestamp)
            move_file(
                output_path,
                os.path.join(self.raw_data_path, 'archive'), new_file_name)
        data.to_csv(output_path, index=0)

    def write_sector_id_data(self, id_dict):
        output_path = os.path.join(self.raw_data_path, 'sector_id_data.json')
        # Check if current file needs to be tagged and moved to archive
        if os.path.isfile(output_path):
            datestamp = dt.datetime.now().strftime('%Y%m%d')
            new_file_name = 'sector_id_data'
            new_file_name += '_moved_{}.json'.format(datestamp)
            move_file(
                output_path,
                os.path.join(self.raw_data_path, 'archive'), new_file_name)
        with open(output_path, 'w') as f:
            json.dump(id_dict, f)


class ImplementationDailyDataPull(object):

    def __init__(self,
                 imp_data_dir=IMPLEMENTATION_DATA_DIR):
        self.raw_data_path = os.path.join(
            _get_data_path(imp_data_dir), 'daily')

    def start(self):
        features = config.features
        dh = DataHandlerSQL()
        end_date = dt.datetime.now().date()
        start_date = end_date - dt.timedelta(days=6)
        for sector in config.sectors:
            ids = config.ids[sector]
            data = dh.get_id_data(ids, features, start_date, end_date)
            self.write_sector_data(data, sector)

    def write_sector_data(self, data, sector):
        output_path = os.path.join(
            self.raw_data_path,
            'raw_daily_data_sector_{}.csv'.format(sector))
        # Check if current file needs to be tagged and moved to archive
        if os.path.isfile(output_path):
            datestamp = dt.datetime.now().strftime('%Y%m%d')
            new_file_name = 'raw_daily_data_sector_{}'.format(sector)
            new_file_name += '_moved_{}.csv'.format(datestamp)
            move_file(
                output_path,
                os.path.join(self.raw_data_path, 'archive'), new_file_name)
        data.to_csv(output_path, index=0)


def move_file(old_path, new_dir, new_name):
    new_path = os.path.join(new_dir, new_name)
    count = 0
    while os.path.exists(new_path):
        count += 1
        name1, name2 = new_name.split('.')
        new_name_2 = '{}_{}.{}'.format(name1, count, name2)
        new_path = os.path.join(new_dir, new_name_2)
    os.rename(old_path, new_path)


def _get_data_path(imp_data_dir):
    path1 = os.path.join(imp_data_dir, 'LongPeadStrategy')
    if not os.path.isdir(path1):
        os.mkdir(path1)
    # Training and daily data
    path2 = os.path.join(path1, 'training')
    if not os.path.isdir(path2):
        os.mkdir(path2)
    path3 = os.path.join(path1, 'daily')
    if not os.path.isdir(path3):
        os.mkdir(path3)
    # Archives for both
    path4 = os.path.join(path2, 'archive')
    if not os.path.isdir(path4):
        os.mkdir(path4)
    path5 = os.path.join(path3, 'archive')
    if not os.path.isdir(path5):
        os.mkdir(path5)
    return path1


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--daily_pull', action='store_true')
    parser.add_argument('--training_pull', action='store_true')
    args = parser.parse_args()

    if args.daily_pull:
        ImplementationDailyDataPull().start()
    if args.training_pull:
        ImplementationTrainingDataPull().start()
