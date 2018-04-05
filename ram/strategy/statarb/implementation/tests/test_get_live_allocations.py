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

from ram.strategy.statarb.implementation.execution.get_live_allocations import *


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
        output['preds'] = [1, 2]
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

    def set_other_data(self):
        pass

    def set_signal_data(self):
        pass


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
        ldata = live_pricing_data[['Date', 'SecCode', 'AdjClose', 'TimeIndex']]
        self.test_data = ldata
        self._features = features

    def get_other_data(self):
        pass

    def get_pricing_data(self):
        pass

    def get_test_dates(self):
        pass


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

    def get_implementation_param_path(self):
        """
        Location of JSON file that is outputted by model selection
        """
        pass

    def process_implementation_params(self):
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
        path2 = os.path.join(path, 'daily_data')
        os.mkdir(path2)
        path3 = os.path.join(path, 'live_pricing')
        os.mkdir(path3)
        # Raw Data
        data = pd.DataFrame()
        yesteday = get_previous_trading_date()
        data['Date'] = [yesteday - dt.timedelta(days=2),
                        yesteday - dt.timedelta(days=1),
                        yesteday] * 2
        data['SecCode'] = [14141.0] * 3 + ['43242'] * 3
        data['AdjClose'] = range(6)
        today = dt.datetime.utcnow().strftime('%Y%m%d')
        data.to_csv(os.path.join(
            path2, today + '_version_0010.csv'), index=False)
        data.to_csv(os.path.join(
            path2, today + '_version_0018.csv'), index=False)
        today = (dt.datetime.utcnow()-dt.timedelta(days=1)).strftime('%Y%m%d')
        data.to_csv(os.path.join(
            path2, today + '_version_0010.csv'), index=False)
        data.to_csv(os.path.join(
            path2, today + '_version_0018.csv'), index=False)
        data.to_csv(os.path.join(path2, 'market_index_data.csv'), index=False)
        # Run map
        params = {'blueprint': {'universe_filter_arguments':
            {'filter': 'AvgDolVol',
             'where': 'MarketCap >= 200 and Close_ between 5 and 500',
             'univ_size': 800},
             'features': ['AdjOpen', 'AdjHigh'],
             'constructor_type': 'universe',
             'output_dir_name': 'StatArbStrategy',
             'universe_date_parameters': {'quarter_frequency_month_offset': 0,
             'start_year': 2004, 'frequency': 'M', 'train_period_length': 3,
             'test_period_length': 2},
             'market_data_params': {'features': ['AdjClose'],
             'seccodes': [50311, 11113, 11097, 11099, 111000]},
             'description': 'Sector 20, Version 002'},
             'prepped_data_version': 'version_0010',
             'column_params': {'response_days': 5, 'holding_period': 9,
             'response_type': 'Simple', 'per_side_count': 30,
             'model': {'max_features': 0.8, 'type': 'tree',
                       'min_samples_leaf': 500}, 'score_var': 'prma_15'},
             'stack_index': 'version_002~version_0010',
             'run_name': 'run_0003_1000',
             'strategy_code_version': 'version_002'}
        run_map = [params, params]
        with open(os.path.join(path1m, 'run_map.json'), 'w') as outfile:
            outfile.write(json.dumps(run_map))
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
        data['SecCode'] = [1234, 4242, 3535]
        data['Ticker'] = ['TRUE', 'IBM', 'GOOGL']
        data['Issuer'] = ['TRUESOMETHING', 'IBM Corp', 'Alphabet']
        data['CLOSE'] = [1, 2, 3]
        data['LAST'] = [1, 2, 'na']
        data['OPEN'] = [1, 2, 3]
        data['HIGH'] = [1, 2, 3]
        data['LOW'] = [1, 2, 'na']
        data['VWAP'] = [1, np.nan, 3]
        data['VOLUME'] = [None, 2, 3]
        data.to_csv(os.path.join(path3, 'prices.csv'), index=None)
        # Scaling data
        data = pd.DataFrame()
        data['SecCode'] = [1234, 4242, 3535]
        data['Date'] = '2010-01-01'
        data['DividendFactor'] = [1, 1.1, 1.2]
        data.to_csv(os.path.join(path3, 'seccode_scaling.csv'), index=None)
        # Bloomberg data
        data = pd.DataFrame()
        data['SecCode'] = [5151, 72727]
        data['DivMultiplier'] = [1.5, 1.0]
        data['SpinoffMultiplier'] = [10, 20.]
        data['SplitMultiplier'] = [2.0, 3.0]
        data.to_csv(os.path.join(path3, 'bloomberg_scaling.csv'), index=None)
        # Ticker mapping
        data = pd.DataFrame()
        data['SecCode'] = ['4242', '5050']
        data['Ticker'] = ['IBM', 'AAPL']
        data.to_csv(os.path.join(path3, 'ticker_mapping.csv'), index=None)
        # Position sheet positions
        positions = pd.DataFrame()
        positions['position'] = ['5050_StatArb_A0123', '1010_StatArb_A0123',
                                 'GE Special Sit', 'CUDA Earnings']
        positions['symbol'] = ['AAPL', 'IBM', 'GE', 'CUDA']
        positions['share_count'] = [1000, -1000, 100, 3333]
        positions['market_price'] = [10, 30, 10, 20]
        positions['position_value'] = [10000, -30000, 330303, -1292]
        positions['daily_pl'] = [20303, -2032, 3, 1]
        positions['position_value_perc_aum'] = [0.003, 0.001, 0.1, 10.]
        today = dt.datetime.utcnow().strftime('%Y%m%d')
        positions.to_csv(os.path.join(self.imp_dir, today + '_positions.csv'),
                         index=0)

    def test_import_raw_data(self):
        result = import_raw_data(self.imp_dir)
        result = result.keys()
        result.sort()
        benchmark = ['market_data', 'version_0010', 'version_0018']
        self.assertListEqual(result, benchmark)

    def test_get_all_data_file_names(self):
        path = os.path.join(self.imp_dir, 'StatArbStrategy', 'daily_data')
        result = get_all_data_file_names(path)
        today = dt.datetime.utcnow().strftime('%Y%m%d')
        yesterday = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y%m%d')
        benchmark = [
            yesterday + '_version_0010.csv',
            yesterday + '_version_0018.csv',
            today + '_version_0010.csv',
            today + '_version_0018.csv'
        ]
        self.assertListEqual(result, benchmark)

    def test_get_max_date_files(self):
        all_files = [
            '20100101_version_0010.csv',
            '20110101_version_0010.csv',
            '20100101_version_0013.csv',
            '20110101_version_0013.csv',
            '20100101_version_0018.csv',
            '20110101_version_0018.csv'
        ]
        result = get_max_date_files(all_files)
        benchmark = [
            '20110101_version_0010.csv',
            '20110101_version_0013.csv',
            '20110101_version_0018.csv'
        ]
        self.assertListEqual(result, benchmark)

    def test_import_format_raw_data(self):
        statarb_path = os.path.join(self.imp_dir, 'StatArbStrategy',
                                    'daily_data')
        todays_files = get_todays_files(statarb_path)
        path = os.path.join(statarb_path, todays_files[0])

        result = import_format_raw_data(path)
        benchmark = pd.DataFrame()
        yesteday = get_previous_trading_date()
        benchmark['Date'] = [yesteday - dt.timedelta(days=2),
                             yesteday - dt.timedelta(days=1),
                             yesteday] * 2
        benchmark['SecCode'] = ['14141'] * 3 + ['43242'] * 3
        benchmark['AdjClose'] = range(6)
        assert_frame_equal(result, benchmark)

    def test_format_data_name(self):
        file_name = '20110101_version_0018.csv'
        result = format_data_name(file_name)
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
        models, params = get_model_files(path)
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
                            'daily_data')
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
        self.assertTrue('run_map.json' in all_files)
        all_files.remove('run_map.json')
        all_files = [x for x in all_files if x.find('skl_model') == 1]
        all_files = [x for x in all_files if x.find('params') == 1]
        self.assertEqual(len(all_files), 0)

    def test_StatArbImplementation_add_daily_data(self):
        raw_data = import_raw_data(self.imp_dir)
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_daily_data(raw_data)

    def test_StatArbImplementation_add_run_map_models(self):
        run_map = import_run_map(self.imp_dir, 'models_0005')
        models_params = import_models_params(self.imp_dir, 'models_0005')
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_run_map_models(run_map, models_params)

    def test_StatArbImplementation_prep_start(self):
        raw_data = import_raw_data(self.imp_dir)
        run_map = import_run_map(self.imp_dir, 'models_0005')
        models_params = import_models_params(self.imp_dir, 'models_0005')
        # TODO: RamexAccounting needs to be implemented here
        positions = {}
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_daily_data(raw_data)
        imp.add_run_map_models(run_map, models_params)
        imp.add_positions(positions)
        imp.prep()
        live_data = import_live_pricing(self.imp_dir)
        # live_data['SecCode'] = [14141, 43242]
        # imp.run_live(live_data)

    def test_import_live_pricing(self):
        result = import_live_pricing(self.imp_dir)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['1234', '4242', '3535']
        benchmark['Ticker'] = ['TRUE', 'IBM', 'GOOGL']
        benchmark['AdjOpen'] = [1, 2, 3.]
        benchmark['AdjHigh'] = [1, 2, 3.]
        benchmark['AdjLow'] = [1, 2, np.nan]
        benchmark['AdjClose'] = [1, 2, np.nan]
        benchmark['AdjVolume'] = [np.nan, 2, 3]
        benchmark['AdjVwap'] = [1, np.nan, 3]
        assert_frame_equal(result, benchmark)

    def test_import_scaling_data(self):
        result = import_scaling_data(self.imp_dir)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['1234', '4242', '3535']
        benchmark['QADirectDividendFactor'] = [1, 1.1, 1.2]
        assert_frame_equal(result, benchmark)

    def test_import_bloomberg_data(self):
        result = import_bloomberg_data(self.imp_dir)
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['5151', '72727']
        benchmark['BbrgDivMultiplier'] = [1.5, 1.0]
        benchmark['BbrgSpinoffMultiplier'] = [10, 20.]
        benchmark['BbrgSplitMultiplier'] = [2.0, 3.0]
        assert_frame_equal(result, benchmark)

    def test_extract_params(self):
        all_params = {'V1': 10, 'V2': 20, 'V3': 3}
        imp = StatArbStrategyTest()
        imp.strategy_init()
        p1 = imp.data.get_args()
        result = extract_params(all_params, p1)
        benchmark = {'V3': 3}
        self.assertDictEqual(result, benchmark)
        p1 = imp.signals.get_args()
        result = extract_params(all_params, p1)
        benchmark = {'V1': 10}
        self.assertDictEqual(result, benchmark)
        p1 = imp.constructor.get_args()
        result = extract_params(all_params, p1)
        benchmark = {'V2': 20}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        if os.path.exists(self.imp_dir):
            shutil.rmtree(self.imp_dir)


if __name__ == '__main__':
    unittest.main()
