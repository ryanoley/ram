import os
import json
import shutil
import unittest
import pandas as pd
import datetime as dt

from ram.data.data_constructor import DataConstructor
from ram.strategy.base import Strategy


class TestStrategy(Strategy):

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        return []

    def get_features(self):
        return ['AvgDolVol', 'PRMA10_Close']

    def get_univ_date_parameters(self):
        return {
            'frequency': 'Q',
            'quarter_frequency_month_offset': 1,
            'train_period_length': 4,
            'test_period_length': 2,
            'start_year': 2017
        }

    def get_univ_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': 10}


class DataConstructor2(DataConstructor):

    def _write_archive_meta_parameters(self):
        """
        Note being tested
        """
        pass


class TestDataConstructor(unittest.TestCase):

    def setUp(self):
        self.prepped_data_dir = os.path.join(os.getenv('GITHUB'), 'ram',
                                             'ram', 'tests')
        self.data_constructor = DataConstructor(
            TestStrategy(),
            prepped_data_dir=self.prepped_data_dir)
        if os.path.isdir(os.path.join(self.prepped_data_dir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.prepped_data_dir, 'TestStrategy'))

    def Xtest_make_output_directory(self):
        self.data_constructor._make_output_directory()
        self.data_constructor._make_output_directory()
        results = os.listdir(os.path.join(self.prepped_data_dir,
                                          'TestStrategy'))
        self.assertListEqual(results, ['archive', 'version_0001',
                                       'version_0002'])

    def Xtest_make_date_iterator(self):
        dc = self.data_constructor
        dc._init_new_run()
        dc._make_date_iterator()
        result = dc._date_iterator[0]
        self.assertEqual(result[0], dt.datetime(2016, 2, 1))
        self.assertEqual(result[1], dt.datetime(2017, 2, 1))
        self.assertEqual(result[2], dt.datetime(2017, 7, 31))

    def Xtest_run(self):
        self.data_constructor.run(prompt_description=False)
        result = os.listdir(os.path.join(self.prepped_data_dir,
                                         'TestStrategy',
                                         'version_0001'))
        result.sort()
        self.assertEquals(result[0], '20170201_data.csv')

    def Xtest_restart_run(self):
        self.data_constructor._init_new_run()
        self.data_constructor._make_output_directory()
        self.data_constructor._write_archive_meta_parameters(False)
        self.data_constructor.run(rerun_version='version_0001')

    def Xtest_check_completeness_final_file(self):
        self.data_constructor._init_new_run()
        self.data_constructor._make_output_directory()
        self.data_constructor._write_archive_meta_parameters(False)
        self.data_constructor._make_date_iterator()
        # Make file that is incomplete, so it is filtered out and
        # overwritten
        all_dates = self.data_constructor.datahandler.get_all_dates()
        df = pd.DataFrame()
        # File 1
        start_date = self.data_constructor._date_iterator[-3][1]
        end_date = self.data_constructor._date_iterator[-3][2]
        data_frame_dates = all_dates[all_dates >= start_date]
        data_frame_dates = data_frame_dates[data_frame_dates <= end_date]
        data = pd.DataFrame({
            'Date': data_frame_dates,
            'AdjClose': range(len(data_frame_dates))
        })
        path = os.path.join(
            self.data_constructor._output_dir,
            '{}_data.csv'.format(start_date.strftime('%Y%m%d'))
        )
        data.to_csv(path)
        # File 2
        start_date = self.data_constructor._date_iterator[-2][1]
        end_date = self.data_constructor._date_iterator[-2][2]
        data_frame_dates = all_dates[all_dates >= start_date]
        data_frame_dates = data_frame_dates[data_frame_dates <= end_date]
        data_frame_dates = data_frame_dates[:-2]
        data = pd.DataFrame({
            'Date': data_frame_dates,
            'AdjClose': range(len(data_frame_dates))
        })
        path = os.path.join(
            self.data_constructor._output_dir,
            '{}_data.csv'.format(start_date.strftime('%Y%m%d'))
        )
        data.to_csv(path)
        self.data_constructor._init_rerun_run(self.data_constructor.version)
        result = self.data_constructor.version_files
        benchmark = ['20170201_data.csv', '20170501_data.csv']
        self.assertListEqual(result, benchmark)
        self.data_constructor._check_completeness_final_file()
        result = self.data_constructor.version_files
        benchmark = ['20170201_data.csv']
        self.assertListEqual(result, benchmark)

    def Xtest_run_index_data(self):
        self.data_constructor._make_output_directory()
        self.data_constructor._make_output_directory()
        self.data_constructor.run_index_data('version_0002')

    def tearDown(self):
        self.data_constructor.datahandler.close_connections()
        if os.path.isdir(os.path.join(self.prepped_data_dir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.prepped_data_dir, 'TestStrategy'))


if __name__ == '__main__':
    unittest.main()
