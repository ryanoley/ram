import os
import json
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

from sklearn.linear_model import LinearRegression

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.base import *
from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer
from ram.strategy.base import StrategyVersionContainer


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

strategy_versions = StrategyVersionContainer()
strategy_versions.add_version('version_001', 'Current implementation')
strategy_versions.add_version('version_002', 'Alternative implementation')

blueprint_container = DataConstructorBlueprintContainer()
bp = DataConstructorBlueprint(constructor_type='universe',
                              description='Test1')
blueprint_container.add_blueprint(bp)
bp = DataConstructorBlueprint(constructor_type='universe',
                              description='Test2')
blueprint_container.add_blueprint(bp)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TestStrategy(Strategy):

    def strategy_init(self):
        pass

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        return {0: {'V1': 1, 'V2': 2}, 1: {'V1': 3, 'V2': 5}}

    def process_raw_data(self, data, time_index, market_data=None):
        pass

    def get_data_blueprint_container(self):
        return blueprint_container

    def get_strategy_source_versions(self):
        return strategy_versions

    def implementation_training(self):
        pass


class TestStrategyBase(unittest.TestCase):

    def setUp(self):
        self.test_data_dir = os.path.join(os.getenv('GITHUB'), 'ram',
                                          'ram', 'test_data')
        self.prepped_data_dir = os.path.join(self.test_data_dir,
                                             'prepped_data')
        self.simulation_output_dir = os.path.join(self.test_data_dir,
                                                  'simulations')
        self.implementation_output_dir = os.path.join(self.test_data_dir,
                                                      'implementation')
        # DELETE LEFTOVER DIRECTORIES
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)

        # CREATE PREPPED DATA DIRECTORY
        os.mkdir(self.test_data_dir)
        os.mkdir(self.prepped_data_dir)
        os.mkdir(self.simulation_output_dir)

        strategy_dir = os.path.join(self.prepped_data_dir, 'TestStrategy')
        data_version_dir = os.path.join(strategy_dir, 'version_0001')

        os.mkdir(strategy_dir)
        os.mkdir(data_version_dir)

        data = pd.DataFrame({
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'],
            'SecCode': [10, 20, 30], 'V1': [3, 4, 5]})
        data.to_csv(os.path.join(data_version_dir, '20100101_data.csv'),
                    index=False)
        data.to_csv(os.path.join(data_version_dir, '20100201_data.csv'),
                    index=False)

        self.strategy = TestStrategy(
            strategy_code_version='version_0002',
            prepped_data_version='version_0001',
            write_flag=True,
            ram_prepped_data_dir=self.prepped_data_dir,
            ram_simulations_dir=self.simulation_output_dir,
            ram_implementation_dir=self.implementation_output_dir)
        # Force this to false regardless of where it is being tested
        self.strategy._gcp_implementation = False

    def test_implementation_output_dir(self):
        self.strategy._create_implementation_output_dir()
        self.strategy._create_implementation_output_dir()
        # Check if directories exist
        result = os.listdir(os.path.join(self.implementation_output_dir,
                                         'TestStrategy', 'trained_models'))
        benchmark = ['models_0001', 'models_0002']
        self.assertListEqual(result, benchmark)
        self.assertEqual(
            os.path.split(self.strategy.implementation_output_dir)[-1],
            'models_0002')
        # Write something
        model = LinearRegression()
        model.fit(X=[[1, 2], [3, 4], [-3, 10]], y=[1, 5, 2])
        benchmark = model.coef_
        self.strategy.implementation_training_write_params_model(
            'run_0001_14', {'param1': [1], 'param2': 3}, model)
        path = os.path.join(self.implementation_output_dir,
                            'TestStrategy', 'trained_models',
                            'models_0002', 'run_0001_14_skl_model.pkl')
        model = joblib.load(path)
        assert_array_equal(model.coef_, benchmark)

    def test_implementation_training_prep(self):
        self.strategy._write_flag = True
        self.strategy._get_prepped_data_file_names()
        # Create four distinct meta files to represent runs
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test')
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test2')
        # Change strategy
        self.strategy.prepped_data_version = 'version_9999'
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test3')
        self.strategy.strategy_code_version = 'version_7676'
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test4')
        top_params = ['TestStrategy_run_0001_10',
                      'TestStrategy_run_0002_20',
                      'TestStrategy_run_0003_30',
                      'TestStrategy_run_0004_40']
        self.strategy._create_implementation_output_dir()
        result = self.strategy.implementation_training_prep(top_params)
        benchmark = pd.DataFrame()
        benchmark['param_name'] = ['run_0001_10', 'run_0002_20',
                                   'run_0003_30', 'run_0004_40']
        benchmark['run_name'] = ['run_0001', 'run_0002',
                                 'run_0003', 'run_0004']
        benchmark['strategy_version'] = ['version_0002', 'version_0002',
                                         'version_0002', 'version_7676']
        benchmark['data_version'] = ['version_0001', 'version_0001',
                                     'version_9999', 'version_9999']
        benchmark['column_name'] = ['10', '20', '30', '40']
        benchmark['stack_index'] = ['version_0002~version_0001',
                                    'version_0002~version_0001',
                                    'version_0002~version_9999',
                                    'version_7676~version_9999']
        assert_frame_equal(result, benchmark)

    def test_get_prepped_data_files(self):
        self.strategy._get_prepped_data_file_names()
        benchmark = ['20100101_data.csv', '20100201_data.csv']
        self.assertListEqual(self.strategy._prepped_data_files, benchmark)

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
        self.assertEqual(os.listdir(self.simulation_output_dir)[0],
                         'TestStrategy')
        result = os.listdir(os.path.join(self.simulation_output_dir,
                                         'TestStrategy'))[0]
        self.assertEqual(result, 'run_0001')
        result = os.listdir(os.path.join(self.simulation_output_dir,
                                         'TestStrategy', 'run_0001'))[0]
        self.assertEqual(result, 'index_outputs')

    def test_create_meta_file(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test')
        result = json.load(open(os.path.join(self.simulation_output_dir,
                                             'TestStrategy', 'run_0001',
                                             'meta.json'), 'r'))
        self.assertEqual(result['completed'], False)
        self.assertTrue('latest_git_commit' in result)
        self.assertTrue('prepped_data_version' in result)
        self.assertTrue('strategy_code_version' in result)
        self.assertTrue('description' in result)
        self.assertTrue('git_branch' in result)
        self.assertTrue('start_time' in result)
        self.assertEqual(len(result), 7)

    def test_write_column_parameters_file(self):

        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._write_column_parameters_file()
        result = json.load(open(os.path.join(self.simulation_output_dir,
                                             'TestStrategy', 'run_0001',
                                             'column_params.json'), 'r'))
        self.assertDictEqual(result, {'0': {'V1': 1, 'V2': 2},
                                      '1': {'V1': 3, 'V2': 5}})

    def test_shutdown_simulation(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test')
        self.strategy._shutdown_simulation()
        result = json.load(open(os.path.join(self.simulation_output_dir,
                                             'TestStrategy', 'run_0001',
                                             'meta.json'), 'r'))
        self.assertEqual(result['completed'], True)

    def test_import_run_meta_for_restart(self):
        self.strategy._write_flag = True
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test')

        # Put in a run file
        dpath = os.path.join(self.strategy.strategy_run_output_dir,
                             'index_outputs',
                             '20100101_returns.csv')
        data = pd.DataFrame(
            {'SecCode': [10, 20, 30], 'V1': [3, 4, 5]},
            index=['2010-01-01', '2010-01-02', '2010-01-03'])
        data.to_csv(dpath)
        dpath = os.path.join(self.strategy.strategy_run_output_dir,
                             'index_outputs',
                             '20100201_returns.csv')
        data = pd.DataFrame(
            {'SecCode': [10, 20], 'V1': [3, 4]},
            index=['2010-01-01', '2010-01-02'])
        data.to_csv(dpath)
        # New Strategy
        strategy = TestStrategy(
            ram_prepped_data_dir=self.prepped_data_dir,
            ram_simulations_dir=self.simulation_output_dir,
            ram_implementation_dir=self.implementation_output_dir)
        strategy._import_run_meta_for_restart('run_0001')
        benchmark = os.path.join(self.simulation_output_dir,
                                 'TestStrategy',
                                 'run_0001')
        self.assertEqual(strategy.strategy_run_output_dir, benchmark)
        strategy._get_prepped_data_file_names()
        result = strategy._prepped_data_files
        benchmark = ['20100101_data.csv', '20100201_data.csv']
        self.assertListEqual(result, benchmark)
        strategy.restart('run_0001')
        result = os.listdir(os.path.join(
            self.strategy.strategy_run_output_dir, 'index_outputs'))
        benchmark = ['20100101_returns.csv']
        self.assertListEqual(result, benchmark)

    def test_get_max_run_time_index_for_restart(self):
        self.strategy._get_prepped_data_file_names()
        self.strategy._create_run_output_dir()
        self.strategy._create_meta_file('Test')
        df = pd.DataFrame(
            {'v1': [10, 20, 30, 40], 'v2': [3, 4, 5, 6]},
            index=['2010-01-01', '2010-01-02', '2010-01-03', '2010-01-04'])
        self.strategy.write_index_results(df, 0)
        # New Strategy
        strategy = TestStrategy(
            write_flag=True,
            ram_prepped_data_dir=self.prepped_data_dir,
            ram_simulations_dir=self.simulation_output_dir,
            ram_implementation_dir=self.implementation_output_dir)
        strategy._init_simulations_output_dir()
        strategy._import_run_meta_for_restart('run_0001')
        strategy._get_prepped_data_file_names()
        strategy._get_max_run_time_index_for_restart()
        self.assertEqual(strategy._restart_time_index, 1)
        df = pd.DataFrame(
            {'v1': [10, 20], 'v2': [3, 4]},
            index=['2010-01-01', '2010-01-02'])
        self.strategy.write_index_results(df, 0)
        strategy = TestStrategy(
            write_flag=True,
            ram_prepped_data_dir=self.prepped_data_dir,
            ram_simulations_dir=self.simulation_output_dir,
            ram_implementation_dir=self.implementation_output_dir)
        strategy._import_run_meta_for_restart('run_0001')
        strategy._get_prepped_data_file_names()
        strategy._get_max_run_time_index_for_restart()
        self.assertEqual(strategy._restart_time_index, 0)
        path = os.path.join(strategy.strategy_run_output_dir, 'index_outputs')
        self.assertEqual(len(os.listdir(path)), 0)

    def test_read_write_json(self):
        meta = {'V1': 2, 'V2': 4}
        out_path = os.path.join(self.prepped_data_dir, 'meta.json')
        write_json(meta, out_path)
        result = read_json(out_path)
        self.assertDictEqual(meta, result)

    def test_read_write_json_cloud(self):
        pass

    def tearDown(self):
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)


if __name__ == '__main__':
    unittest.main()
