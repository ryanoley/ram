import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.implementation.prep_data import *
from ram.strategy.statarb.implementation.tests.make_test_data import *


class TestPrepData(unittest.TestCase):

    def setUp(self):
        data = ImplementationDataTestSuite()
        data.make_data()
        self.data_dir = data.data_dir
        # Dates
        dates = get_trading_dates()
        self.yesterday = dates[0]
        self.today = dates[1]

    def test_get_trading_dates(self):
        today = dt.date.today()
        self.assertTrue(self.yesterday < today)
        self.assertEqual(self.today, today)

    def test_get_qadirect_data_info(self):
        result = get_qadirect_data_info(self.yesterday, self.data_dir)
        benchmark = pd.DataFrame()
        benchmark['Desc'] = ['Data file: version_0010.csv',
                             'Data file: version_0018.csv']
        benchmark['Message'] = '*'
        assert_frame_equal(result, benchmark)

    def test_check_new_sizes(self):
        check_new_sizes(self.yesterday, self.data_dir, 'models_0005')
        path = os.path.join(self.data_dir,
                            'StatArbStrategy',
                            'archive',
                            'size_containers')
        result = os.listdir(path)
        prefix = self.yesterday.strftime('%Y%m%d')
        file_name = '{}_size_containers_NEW_MODEL.json'.format(prefix)
        self.assertTrue(file_name in result)
        # Check meta file
        path = os.path.join(self.data_dir, 'StatArbStrategy',
                            'trained_models', 'models_0005', 'meta.json')
        result = json.load(open(path, 'r'))
        self.assertTrue(result['execution_confirm'])

    def test_check_size_containers(self):
        path = os.path.join(self.data_dir, 'StatArbStrategy', 'live')
        all_files = os.listdir(path)

        # self.assertFalse('size_containers.json' in all_files)
        result = check_size_containers(self.yesterday,
                                       self.data_dir,
                                       'models_0005')
        benchmark = pd.DataFrame()
        benchmark['Desc'] = ['Size containers']
        benchmark['Message'] = ['[INFO] New model SizeContainers being used']
        assert_frame_equal(result, benchmark)
        path = os.path.join(self.data_dir, 'StatArbStrategy', 'live')
        all_files = os.listdir(path)
        self.assertTrue('size_containers.json' in all_files)

    def test_get_short_sell_killed_seccodes(self):
        result = get_short_sell_killed_seccodes(self.yesterday,
                                                data_dir=self.data_dir)
        dt_str = '{d.month}.{d.day}.{d:%y}'.format(d=self.yesterday)
        message = '[ERROR] no locate file for {} found'.format(dt_str)
        benchmark = pd.DataFrame()
        benchmark['Desc'] = ['Short Locates']
        benchmark['Message'] = [message]
        assert_frame_equal(result, benchmark)

        result = get_short_sell_killed_seccodes(self.today,
                                                data_dir=self.data_dir)
        benchmark = pd.DataFrame()
        benchmark['Desc'] = ['Short Locates']
        benchmark['Message'] = ['[INFO] 1 securities no map to SecCodes']
        assert_frame_equal(result, benchmark)

        write_path = os.path.join(self.data_dir, 'short_sell_kill_list.csv')
        self.assertTrue(os.path.exists(write_path))

    def tearDown(self):
        ImplementationDataTestSuite().delete_data()


if __name__ == '__main__':
    unittest.main()
