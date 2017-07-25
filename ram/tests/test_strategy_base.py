import os
import json
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.base import Strategy


class TestStrategy(Strategy):

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        return {0: {'V1': 1, 'V2': 2}, 1: {'V1': 3, 'V2': 5}}

    def get_features(self):
        return ['AvgDolVol', 'PRMA10_Close']

    def get_date_parameters(self):
        return {
            'frequency': 'Q',
            'test_period_length': 2,
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
        self.output_dir = os.path.join(
            os.getenv('GITHUB'), 'ram', 'ram', 'tests', 'simulations')

        # DELETE LEFTOVER DIRECTORIES
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        if os.path.exists(self.prepped_data_dir):
            shutil.rmtree(self.prepped_data_dir)

        # CREATE PREPPED DATA DIRECTORY
        os.mkdir(self.prepped_data_dir)

        strategy_dir = os.path.join(self.prepped_data_dir, 'TestStrategy')
        data_version_dir = os.path.join(strategy_dir, 'version_001')

        os.mkdir(strategy_dir)
        os.mkdir(data_version_dir)

        data = pd.DataFrame({
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'],
            'SecCode': [10, 20, 30], 'V1': [3, 4, 5]})
        data.to_csv(os.path.join(data_version_dir, '20100101_data.csv'),
                    index=False)
        data.to_csv(os.path.join(data_version_dir, '20100201_data.csv'),
                    index=False)

        meta = {
            'features': ['F1', 'F2'],
            'date_parameters_univ': {
                'train_period_length': 2,
                'test_period_length': 2,
                'frequency': 'Q',
                'start_year': 2000
            },
            'filter_args_univ': {
                'filter': 'AvgDolVol',
                'where': 'Market Cap >= 200',
                'univ_size': 10
            },
            'strategy_name': 'TestStrategy',
            'version': 'version_0001',
            'git_branch': 'master',
            'git_commit': 'adfad14324213',
        }
        with open(os.path.join(data_version_dir, 'meta.json'), 'w') as outfile:
            json.dump(meta, outfile)
        outfile.close()

        self.strategy = TestStrategy(
            prepped_data_version='version_001',
            write_flag=False,
            prepped_data_dir=self.prepped_data_dir,
            simulation_output_dir=self.output_dir)

    def test_get_prepped_data_files(self):
        self.strategy._get_prepped_data_file_names()
        benchmark = ['20100101_data.csv', '20100201_data.csv']
        self.assertListEqual(self.strategy._prepped_data_files, benchmark)

    def test_print_prepped_data_meta(self):
        self.strategy._print_prepped_data_meta()

    def test_read_data_from_index(self):
        result = self.strategy.read_data_from_index(1)
        benchmark = pd.DataFrame()
        benchmark['Date'] = convert_date_array(
            ['2010-01-01', '2010-01-02', '2010-01-03'])
        benchmark['SecCode'] = ['10', '20', '30']
        benchmark['V1'] = [3, 4, 5]
        assert_frame_equal(result, benchmark)

    def test_create_run_output_dir(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.assertEqual(os.listdir(self.output_dir)[0], 'TestStrategy')
        result = os.listdir(os.path.join(self.output_dir, 'TestStrategy'))[0]
        self.assertEqual(result, 'run_0001')
        result = os.listdir(os.path.join(self.output_dir, 'TestStrategy',
                            'run_0001'))[0]
        self.assertEqual(result, 'index_outputs')

    def test_create_meta_file(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file(False)
        result = json.load(open(os.path.join(self.output_dir,
                                             'TestStrategy', 'run_0001',
                                             'meta.json'), 'r'))
        self.assertEqual(result['completed'], False)
        self.assertTrue('latest_git_commit' in result)
        self.assertTrue('prepped_data_version' in result)
        self.assertTrue('description' in result)
        self.assertTrue('git_branch' in result)
        self.assertTrue('start_time' in result)
        self.assertEqual(len(result), 6)

    def test_write_column_parameters_file(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._write_column_parameters_file()
        result = json.load(open(os.path.join(self.output_dir,
                                             'TestStrategy', 'run_0001',
                                             'column_params.json'), 'r'))
        self.assertDictEqual(result, {'0': {'V1': 1, 'V2': 2},
                                      '1': {'V1': 3, 'V2': 5}})

    def test_shutdown_simulation(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file(False)
        self.strategy._shutdown_simulation()
        result = json.load(open(os.path.join(self.output_dir,
                                             'TestStrategy', 'run_0001',
                                             'meta.json'), 'r'))
        self.assertEqual(result['completed'], True)

    def tearDown(self):
        shutil.rmtree(self.prepped_data_dir)
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)


if __name__ == '__main__':
    unittest.main()
