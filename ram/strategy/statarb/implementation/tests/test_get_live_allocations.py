import os
import pickle
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config

from ram.strategy.base import Strategy

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor
from ram.strategy.statarb.abstract.data_container import BaseDataContainer
from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator

from sklearn.linear_model import LinearRegression

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.implementation.get_live_allocations import *
from ram.strategy.statarb.implementation.get_live_allocations import \
    _get_max_date_files, _get_all_raw_data_file_names, \
    _import_format_raw_data, _format_raw_data_name, _get_model_files, \
    _extract_params, _add_sizes


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Signals(BaseSignalGenerator):

    def get_args(self):
        return {'V1': [1, 2]}

    def set_args(self, **kwargs):
        pass

    def set_features(self, features):
        pass

    def set_train_data(self, train_data):
        pass

    def set_train_responses(self, train_responses):
        pass

    def set_test_data(self, test_data):
        self._test_data = test_data

    def fit_model(self):
        pass

    def get_model(self):
        pass

    def set_model(self, model):
        self.skl_model = model

    def get_signals(self):
        output = self._test_data[['SecCode', 'Date']].copy()
        output['preds'] = [1, 2, 3]
        return output


class PortfolioConstructorTest(BasePortfolioConstructor):

    def get_args(self):
        return {'V2': [8, 9]}

    def set_args(self, **kwargs):
        pass

    def set_signals_constructor_data(self, signals, data):
        pass

    def get_day_position_sizes(self, date, signals):
        return signals


class DataContainerTest(BaseDataContainer):

    def get_args(self):
        return {'V3': [111, 3239]}

    def set_args(self, **kwargs):
        pass

    def get_train_data(self):
        pass

    def get_train_responses(self):
        pass

    def get_train_features(self):
        return self._features

    def get_test_data(self):
        return self.test_data

    def get_constructor_data(self):
        return self._constructor_data

    def process_training_data(self, data, time_index):
        pass

    def process_training_market_data(self, data):
        pass

    def prep_live_data(self, data, market_data):
        data['TimeIndex'] = -1
        features = ['AdjClose']
        self._live_prepped_data = {}
        self._live_prepped_data['data'] = data
        self._live_prepped_data['features'] = features
        self._constructor_data = {}

    def process_live_data(self, live_pricing_data):
        """
        Notes:
        HOW DO WE HANDLE LIVE SPLITS??
        """
        data = self._live_prepped_data['data']
        features = self._live_prepped_data['features']
        del self._live_prepped_data
        live_pricing_data['Date'] = dt.datetime.utcnow().date()
        live_pricing_data['TimeIndex'] = -1
        live_pricing_data['AdjClose'] = live_pricing_data.LAST
        ldata = live_pricing_data[['Date', 'SecCode', 'AdjClose', 'TimeIndex']]
        self.test_data = ldata
        self._features = features


class StatArbStrategyTest(Strategy):

    def strategy_init(self):
        self.data = DataContainerTest()
        self.signals = Signals()
        self.constructor = PortfolioConstructorTest()

    def get_data_blueprint_container(self):
        pass

    def get_strategy_source_versions(self):
        pass

    def process_raw_data(self, data, time_index, market_data=None):
        pass

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        pass

    def implementation_training(self):
        pass


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        path3 = os.path.join(path, 'live_pricing')
        os.mkdir(path3)
        # Raw Data
        data = pd.DataFrame()
        data['Date'] = ['2010-01-01', '2010-01-02', '2010-01-03'] * 2
        data['SecCode'] = [14141.0] * 3 + ['43242'] * 3
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
        params = {'V1': 3, 'V2': 10, 'V3': 3}
        path = os.path.join(path1m, 'run_0003_1000_params.json')
        with open(path, 'w') as outfile:
            outfile.write(json.dumps(params))
        path = os.path.join(path1m, 'run_009_12_params.json')
        with open(path, 'w') as outfile:
            outfile.write(json.dumps(params))
        # Live pricing
        data = pd.DataFrame()
        data['Symbol'] = ['AAPL', 'IBM', 'GOOGL']
        data['Name'] = ['Apple', 'IBM Corp', 'Alphabet']
        data['CLOSE'] = [1, 2, 3]
        data['LAST'] = [1, 2, 3]
        data['OPEN'] = [1, 2, 3]
        data['HIGH'] = [1, 2, 3]
        data['LOW'] = [1, 2, 3]
        data['VWAP'] = [1, 2, 3]
        data['VOLUME'] = [1, 2, 3]
        data.to_csv(os.path.join(path3, 'live_prices.csv'), index=None)

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

    def test_check_production_implementation_directories(self):
        # Don't test if on cloud instance
        if config.GCP_CLOUD_IMPLEMENTATION:
            return
        # Check if connected to server
        if not os.path.isdir(config.IMPLEMENTATION_DATA_DIR):
            return
        # Check raw data
        path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'daily_raw_data')
        all_files = os.listdir(path)
        self.assertTrue('market_index_data.csv' in all_files)
        all_files.remove('market_index_data.csv')
        all_files = [x for x in all_files if x.find('blueprint') == 1]
        self.assertEqual(len(all_files), 0)
        path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy',
                            'trained_models',
                            statarb_config.trained_models_dir_name)
        all_files = os.listdir(path)
        self.assertTrue('run_map.csv' in all_files)
        all_files.remove('run_map.csv')
        all_files = [x for x in all_files if x.find('skl_model') == 1]
        all_files = [x for x in all_files if x.find('params') == 1]
        self.assertEqual(len(all_files), 0)

    def test_StatArbImplementation_add_raw_data(self):
        raw_data = import_raw_data(self.imp_dir)
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_raw_data(raw_data)

    def test_StatArbImplementation_add_run_map(self):
        run_map = import_run_map(self.imp_dir, 'models_0005')
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_run_map(run_map)

    def test_StatArbImplementation_add_models_params(self):
        models_params = import_models_params(self.imp_dir, 'models_0005')
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_models_params(models_params)

    def test_StatArbImplementation_prep_start(self):
        run_map = import_run_map(self.imp_dir, 'models_0005')
        raw_data = import_raw_data(self.imp_dir)
        models_params = import_models_params(self.imp_dir, 'models_0005')
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_run_map(run_map)
        imp.add_raw_data(raw_data)
        imp.add_models_params(models_params)
        imp.prep()
        live_data = import_live_pricing(self.imp_dir)
        live_data['SecCode'] = ['14141', '43242', '9999']  # Assume merged
        imp.run_live(live_data)

    def test_import_live_pricing(self):
        result = import_live_pricing(self.imp_dir)

    def test_extract_params(self):
        all_params = {'V1': 10, 'V2': 20, 'V3': 3}
        imp = StatArbStrategyTest()
        p1 = imp.data.get_args()
        result = _extract_params(all_params, p1)
        benchmark = {'V3': 3}
        self.assertDictEqual(result, benchmark)
        p1 = imp.signals.get_args()
        result = _extract_params(all_params, p1)
        benchmark = {'V1': 10}
        self.assertDictEqual(result, benchmark)
        p1 = imp.constructor.get_args()
        result = _extract_params(all_params, p1)
        benchmark = {'V2': 20}
        self.assertDictEqual(result, benchmark)

    def test_add_sizes(self):
        all_sizes = {}
        model_sizes = {'a': 100, 'b': -100}
        all_sizes = _add_sizes(all_sizes, model_sizes)
        model_sizes = {'a': 100, 'b': -100}
        all_sizes = _add_sizes(all_sizes, model_sizes)
        model_sizes = {'a': -250, 'b': -50, 'c': 300}
        all_sizes = _add_sizes(all_sizes, model_sizes)
        benchmark = {'a': -50, 'b': -250, 'c': 300}
        self.assertDictEqual(all_sizes, benchmark)

    def tearDown(self):
        if os.path.exists(self.imp_dir):
            shutil.rmtree(self.imp_dir)


if __name__ == '__main__':
    unittest.main()
