import os
import json
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

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


class TestStrategyBase(unittest.TestCase):

    def setUp(self):
        self.prepped_data_dir = os.path.join(
            os.getenv('GITHUB'), 'ram', 'ram', 'tests', 'prepped_data')
        strategy_dir = os.path.join(self.prepped_data_dir, 'TestStrategy')
        version_dir = os.path.join(strategy_dir, 'version_001')
        if os.path.exists(self.prepped_data_dir):
            shutil.rmtree(self.prepped_data_dir)
        # Make some fake data
        os.mkdir(self.prepped_data_dir)
        os.mkdir(strategy_dir)
        os.mkdir(version_dir)
        data = pd.DataFrame({
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'],
            'SecCode': [10, 20, 30], 'V1': [3, 4, 5]})
        data.to_csv(os.path.join(version_dir, '20100101_data.csv'))
        data.to_csv(os.path.join(version_dir, '20100201_data.csv'))
        self.strategy = TestStrategy('version_001', False)
        self.strategy._prepped_data_dir = version_dir
        self.strategy._output_dir = self.prepped_data_dir
        meta = {'start_year': 2000,
                'features': ['F1', 'F2'],
                'train_period_len': 2,
                'test_period_len': 2,
                'frequency': 'Q',
                'filter_args': {'filter': 'AvgDolVol',
                                'where': 'Market Cap >= 200',
                                'univ_size': 10},
                'strategy_name': 'TestSTrategy',
                'version': 'version_0002',
                'git_branch': 'master',
                'git_commit': 'adfad14324213'}
        with open(os.path.join(version_dir, 'meta.json'), 'w') as outfile:
            json.dump(meta, outfile)
        outfile.close()

    def test_data_files(self):
        self.strategy._get_data_file_names()
        benchmark = ['20100101_data.csv', '20100201_data.csv']
        self.assertListEqual(self.strategy._data_files, benchmark)

    def test_print_prepped_data_meta(self):
        self.strategy._print_prepped_data_meta()

    def test_read_data_from_index(self):
        self.strategy._get_data_file_names()
        result = self.strategy.read_data_from_index(1)

    def tearDown(self):
        shutil.rmtree(self.prepped_data_dir)


if __name__ == '__main__':
    unittest.main()
