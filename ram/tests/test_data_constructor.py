import os
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

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 4,
            'start_year': 2017
        }

    def get_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': 10}


class TestDataConstructor(unittest.TestCase):

    def setUp(self):
        self.ddir = os.path.join(os.getenv('GITHUB'), 'ram', 'ram', 'tests')

    def test_make_output_directory(self):
        dc = DataConstructor(TestStrategy())
        # Manually override write directory
        dc._prepped_data_dir = os.path.join(self.ddir, 'TestStrategy')
        if os.path.isdir(os.path.join(self.ddir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))
        dc._make_output_directory()
        dc._make_output_directory()
        results = os.listdir(os.path.join(self.ddir, 'TestStrategy'))
        self.assertListEqual(results, ['archive', 'version_0001',
                                       'version_0002'])
        shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))

    def test_make_date_iterator(self):
        dc = DataConstructor(TestStrategy())
        dc._make_date_iterator()
        result = dc._date_iterator[0]
        self.assertEqual(result[0], dt.datetime(2016, 1, 1))
        self.assertEqual(result[1], dt.datetime(2017, 1, 1))
        self.assertEqual(result[2], dt.datetime(2017, 3, 31))

    def test_run(self):
        # Continue with test
        dc = DataConstructor(TestStrategy())
        dc._prepped_data_dir = os.path.join(self.ddir, 'TestStrategy')
        if os.path.isdir(os.path.join(self.ddir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))
        dc.run()
        result = os.listdir(os.path.join(self.ddir, 'TestStrategy',
                                         'version_0001'))
        self.assertEquals(result[0], '20170101_data.csv')
        shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
