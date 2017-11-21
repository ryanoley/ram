# import os
# import json
# import shutil
# import numpy as np
# import pandas as pd
# import datetime as dt
# from tqdm import tqdm

# from ram.config import IMPLEMENTATION_DATA_DIR
# from ram.data.data_handler_sql import DataHandlerSQL
# from ram.strategy.long_pead.implementation import config


# class ImplementationDailyDataPull(object):

#     def __init__(self,
#                  imp_data_dir=IMPLEMENTATION_DATA_DIR):
#         self.raw_data_path = os.path.join(
#             _get_data_path(imp_data_dir), 'daily')

#     def start(self):
#         features = config.features
#         dh = DataHandlerSQL()
#         end_date = dt.datetime.now().date()
#         start_date = end_date - dt.timedelta(days=6)
#         for sector in config.sectors:
#             ids = config.ids[sector]
#             data = dh.get_id_data(ids, features, start_date, end_date)
#             self.write_sector_data(data, sector)

#     def write_sector_data(self, data, sector):
#         output_path = os.path.join(
#             self.raw_data_path,
#             'raw_data_sector_{}.csv'.format(sector))
#         # Check if current file needs to be tagged and moved to archive
#         if os.path.isfile(output_path):
#             datestamp = dt.datetime.now().strftime('%Y%m%d')
#             new_file_name = 'raw_data_sector_{}'.format(sector)
#             new_file_name += '_moved_{}.csv'.format(datestamp)
#             move_file(
#                 output_path,
#                 os.path.join(self.raw_data_path, 'archive'), new_file_name)
#         data.to_csv(output_path, index=0)


# def move_file(old_path, new_dir, new_name):
#     new_path = os.path.join(new_dir, new_name)
#     count = 0
#     while os.path.exists(new_path):
#         count += 1
#         name1, name2 = new_name.split('.')
#         new_name_2 = '{}_{}.{}'.format(name1, count, name2)
#         new_path = os.path.join(new_dir, new_name_2)
#     os.rename(old_path, new_path)


# def _get_data_path(imp_data_dir):
#     path1 = os.path.join(imp_data_dir, 'LongPeadStrategy')
#     if not os.path.isdir(path1):
#         os.mkdir(path1)
#     path2 = os.path.join(path1, 'daily')
#     if not os.path.isdir(path2):
#         os.mkdir(path2)
#     # Archives for both
#     path3 = os.path.join(path2, 'archive')
#     if not os.path.isdir(path3):
#         os.mkdir(path3)
#     return path1


# if __name__ == '__main__':

#     import argparse

#     parser = argparse.ArgumentParser()
#     parser.add_argument('--daily_pull', action='store_true')
#     args = parser.parse_args()

#     if args.daily_pull:
#         ImplementationDailyDataPull().start()
