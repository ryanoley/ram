import os
import json
import shutil
import unittest
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

    def get_filter_args(self):
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

    def test_make_output_directory(self):
        self.data_constructor._make_output_directory()
        self.data_constructor._make_output_directory()
        results = os.listdir(os.path.join(self.prepped_data_dir,
                                          'TestStrategy'))
        self.assertListEqual(results, ['archive', 'version_0001',
                                       'version_0002'])

    def test_make_date_iterator(self):
        dc = self.data_constructor
        dc._init_new_run()
        dc._make_date_iterator()
        result = dc._date_iterator[0]
        self.assertEqual(result[0], dt.datetime(2016, 2, 1))
        self.assertEqual(result[1], dt.datetime(2017, 2, 1))
        self.assertEqual(result[2], dt.datetime(2017, 7, 31))

    def test_run(self):
        self.data_constructor.run(prompt_description=False)
        result = os.listdir(os.path.join(self.prepped_data_dir,
                                         'TestStrategy',
                                         'version_0001'))
        result.sort()
        self.assertEquals(result[0], '20170201_data.csv')

    def test_restart_run(self):
        self.data_constructor._init_new_run()
        self.data_constructor._make_output_directory()
        self.data_constructor._write_archive_meta_parameters(False)
        self.data_constructor.run(rerun_version='version_0001')

    def Xtest_run_index_data(self):
        self.data_constructor._make_output_directory()
        self.data_constructor._make_output_directory()
        self.data_constructor.run_index_data('version_0002')

    def tearDown(self):
        if os.path.isdir(os.path.join(self.prepped_data_dir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.prepped_data_dir, 'TestStrategy'))


if __name__ == '__main__':
    unittest.main()
