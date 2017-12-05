import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt
import pickle

from sklearn.linear_model import LinearRegression

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.implementation.get_live_allocations import *
from ram.strategy.statarb.implementation.get_live_allocations import \
    _get_max_date_files, _get_all_raw_data_file_names, \
    _import_format_raw_data, _format_raw_data_name, _get_model_files


class TestGetLiveAllocations(unittest.TestCase):

    def setUp(self):
        # Directory setup
        self.imp_dir = os.path.join(os.getenv('GITHUB'), 'ram',
                                    'ram', 'test_data')
        # DELETE LEFTOVER DIRECTORIES
        if os.path.exists(self.imp_dir):
            shutil.rmtree(self.imp_dir)
        os.mkdir(self.imp_dir)
        path = os.path.join(self.imp_dir, 'StatArbStrategy')
        os.mkdir(path)
        # Create directories for get_live_allocations
        path1 = os.path.join(path, 'trained_models')
        os.mkdir(path1)
        path1m = os.path.join(path1, 'models_0005')
        os.mkdir(path1m)
        path2 = os.path.join(path, 'daily_raw_data')
        os.mkdir(path2)
        # Raw Data
        data = pd.DataFrame()
        data['Date'] = ['2010-01-01', '2010-01-02', '2010-01-03'] * 2
        data['SecCode'] = [14141.3] * 3 + ['43242'] * 3
        data['AdjClose'] = range(6)
        data.to_csv(os.path.join(
            path2, '20100101_current_blueprint_version_0010.csv'), index=False)
        data.to_csv(os.path.join(
            path2, '20100102_current_blueprint_version_0010.csv'), index=False)
        data.to_csv(os.path.join(
            path2, '20100101_current_blueprint_version_0018.csv'), index=False)
        data.to_csv(os.path.join(
            path2, '20100102_current_blueprint_version_0018.csv'), index=False)
        data.to_csv(os.path.join(path2, 'market_index_data.csv'), index=False)
        # Run map
        data = pd.DataFrame()
        data['param_name'] = ['run_0003_1000', 'run_009_12']
        data['run_name'] = ['run_0003', 'run_009']
        data['strategy_version'] = ['version_0001'] * 2
        data['data_version'] = ['version_0010', 'version_0018']
        data['column_name'] = [1000, 12]
        data['stack_index'] = ['version_0001~version_0010',
                               'version_0001~version_0018']
        data.to_csv(os.path.join(path1m, 'run_map.csv'), index=False)
        # Create sklearn model and params
        model = LinearRegression()
        X = np.random.randn(100, 3)
        y = np.random.randn(100)
        model.fit(X=X, y=y)
        path = os.path.join(path1m, 'run_0003_1000_skl_model.pkl')
        with open(path, 'w') as outfile:
            outfile.write(pickle.dumps(model))
        path = os.path.join(path1m, 'run_009_12_skl_model.pkl')
        with open(path, 'w') as outfile:
            outfile.write(pickle.dumps(model))
        params = {'v1': 3, 'v2': 10}
        path = os.path.join(path1m, 'run_0003_1000_params.json')
        with open(path, 'w') as outfile:
            outfile.write(json.dumps(params))
        path = os.path.join(path1m, 'run_009_12_params.json')
        with open(path, 'w') as outfile:
            outfile.write(json.dumps(params))

    def test_import_raw_data(self):
        result = import_raw_data(self.imp_dir)
        result = result.keys()
        result.sort()
        benchmark = ['market_data', 'version_0010', 'version_0018']
        self.assertListEqual(result, benchmark)

    def test_get_all_raw_data_file_names(self):
        path = os.path.join(self.imp_dir, 'StatArbStrategy', 'daily_raw_data')
        result = _get_all_raw_data_file_names(path)
        benchmark = [
            '20100101_current_blueprint_version_0010.csv',
            '20100101_current_blueprint_version_0018.csv',
            '20100102_current_blueprint_version_0010.csv',
            '20100102_current_blueprint_version_0018.csv'
        ]
        self.assertListEqual(result, benchmark)

    def test_get_max_date_files(self):
        all_files = [
            '20100101_current_blueprint_version_0010.csv',
            '20110101_current_blueprint_version_0010.csv',
            '20100101_current_blueprint_version_0013.csv',
            '20110101_current_blueprint_version_0013.csv',
            '20100101_current_blueprint_version_0018.csv',
            '20110101_current_blueprint_version_0018.csv'
        ]
        result = _get_max_date_files(all_files)
        benchmark = [
            '20110101_current_blueprint_version_0010.csv',
            '20110101_current_blueprint_version_0013.csv',
            '20110101_current_blueprint_version_0018.csv'
        ]
        self.assertListEqual(result, benchmark)

    def test_import_format_raw_data(self):
        path = os.path.join(self.imp_dir, 'StatArbStrategy', 'daily_raw_data',
                            '20100101_current_blueprint_version_0010.csv')
        result = _import_format_raw_data(path)
        benchmark = pd.DataFrame()
        benchmark['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2),
                             dt.date(2010, 1, 3)] * 2
        benchmark['SecCode'] = ['14141'] * 3 + ['43242'] * 3
        benchmark['AdjClose'] = range(6)
        assert_frame_equal(result, benchmark)

    def test_format_raw_data_name(self):
        file_name = '20110101_current_blueprint_version_0018.csv'
        result = _format_raw_data_name(file_name)
        benchmark = 'version_0018'
        self.assertEqual(result, benchmark)

    def test_import_run_map(self):
        result = import_run_map(self.imp_dir, 'models_0005')

    def test_import_model_params(self):
        output = import_models_params(self.imp_dir, 'models_0005')
        result = output.keys()
        result.sort()
        benchmark = ['run_0003_1000', 'run_009_12']
        self.assertListEqual(result, benchmark)
        result = output['run_0003_1000'].keys()
        result.sort()
        benchmark = ['model', 'params']
        self.assertListEqual(result, benchmark)
        result = output['run_0003_1000']['model']
        self.assertIsInstance(result, LinearRegression)
        result = output['run_0003_1000']['params']
        self.assertIsInstance(result, dict)

    def test_get_model_files(self):
        path = os.path.join(self.imp_dir, 'StatArbStrategy',
                            'trained_models', 'models_0005')
        models, params = _get_model_files(path)
        benchmark = ['run_0003_1000_skl_model.pkl', 'run_009_12_skl_model.pkl']
        self.assertListEqual(models, benchmark)
        benchmark = ['run_0003_1000_params.json', 'run_009_12_params.json']
        self.assertListEqual(params, benchmark)

    def tearDown(self):
        if os.path.exists(self.imp_dir):
            shutil.rmtree(self.imp_dir)


if __name__ == '__main__':
    unittest.main()
