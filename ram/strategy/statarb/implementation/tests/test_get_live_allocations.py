import os
import pickle
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from sklearn.linear_model import LinearRegression

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram import config
from ram.strategy.statarb import statarb_config

# Strategy
from ram.strategy.base import Strategy

from ram.strategy.statarb.abstract.portfolio_constructor import *
from ram.strategy.statarb.abstract.data_container import *
from ram.strategy.statarb.abstract.signal_generator import *

from ram.strategy.statarb.sizes import SizeContainer

from ram.strategy.statarb.implementation.tests.make_test_data import *
from ram.strategy.statarb.implementation.get_live_allocations import *

from ramex.orders.orders import MOCOrder


###############################################################################
# Basic Strategy Class
###############################################################################

class SignalsTest(BaseSignalGenerator):

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

    def __init__(self):
        self._size_containers = {}

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
        self.signals = SignalsTest()
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


###############################################################################
# Tests
###############################################################################

class TestGetLiveAllocations(unittest.TestCase):

    def setUp(self):
        data = ImplementationDataTestSuite()
        data.make_data()
        self.data_dir = data.data_dir
        sizes = SizeContainer(2)
        sizes.update_sizes({'A': 100, 'B': 200}, dt.date.today())
        self.size_containers = {'run_0003_1000': sizes}

    def test_import_raw_data(self):
        result = import_raw_data(self.data_dir)
        result = result.keys()
        result.sort()
        benchmark = ['market_data', 'version_0010', 'version_0018']
        self.assertListEqual(result, benchmark)

    def test_get_todays_file_names(self):
        result = get_todays_file_names(self.data_dir)
        today = dt.datetime.utcnow().strftime('%Y%m%d')
        benchmark = ['version_0010.csv', 'version_0018.csv']
        self.assertListEqual(result, benchmark)

    def test_import_format_raw_data(self):
        statarb_path = os.path.join(self.data_dir)
        todays_files = get_todays_file_names(statarb_path)
        result = import_format_raw_data(todays_files[0], statarb_path)
        benchmark = pd.DataFrame()
        yesteday = get_previous_trading_date()
        benchmark['Date'] = [yesteday - dt.timedelta(days=2),
                             yesteday - dt.timedelta(days=1),
                             yesteday] * 2
        benchmark['SecCode'] = ['14141'] * 3 + ['43242'] * 3
        benchmark['AdjClose'] = range(6)
        assert_frame_equal(result, benchmark)

    def test_clean_data_file_name(self):
        file_name = '20110101_version_0018.csv'
        result = clean_data_file_name(file_name)
        benchmark = 'version_0018'
        self.assertEqual(result, benchmark)

    def test_import_run_map(self):
        result = import_run_map()

    def test_import_model_params(self):
        output = import_models_params(self.data_dir, 'models_0005')
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
        path = os.path.join(self.data_dir, 'StatArbStrategy',
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
                            'live')
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
        raw_data = import_raw_data(self.data_dir)
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_daily_data(raw_data)

    def test_StatArbImplementation_add_run_map_models(self):
        run_map = import_run_map(self.data_dir, 'models_0005')
        models_params = import_models_params(self.data_dir, 'models_0005')
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_run_map_models(run_map, models_params)

    def test_StatArbImplementation_prep_start(self):
        raw_data = import_raw_data(self.data_dir)
        run_map = import_run_map(self.data_dir, 'models_0005')
        models_params = import_models_params(self.data_dir, 'models_0005')
        # TODO: RamexAccounting needs to be implemented here
        positions = {}
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_daily_data(raw_data)
        imp.add_run_map_models(run_map, models_params)
        imp.add_size_containers(self.size_containers)
        # import pdb; pdb.set_trace()
        imp.prep()
        ddir = os.path.join(self.data_dir, 'live_prices')
        live_data = import_live_pricing(ddir, self.data_dir)
        # live_data['SecCode'] = [14141, 43242]
        # imp.run_live(live_data)

    def test_import_live_pricing(self):
        ddir = os.path.join(self.data_dir, 'live_prices')
        result = import_live_pricing(ddir, self.data_dir)
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
        # Check if written to archive
        path = os.path.join(self.data_dir, 'StatArbStrategy',
                            'archive', 'live_pricing')
        result = os.listdir(path)
        prefix = dt.date.today().strftime('%Y%m%d')
        benchmark = ['{}_live_pricing.csv'.format(prefix)]
        self.assertListEqual(result, benchmark)

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

    def test_make_orders(self):
        orders = pd.DataFrame()
        orders['SecCode'] = ['A', 'A', 'B', 'B']
        orders['Dollars'] = [100, 100, 200, -140]
        orders['Strategy'] = ['Strat1', 'Strat2'] * 2
        orders['Ticker'] = ['A', 'A', 'B', 'B']
        orders['RClose'] = [10, 10, 20, 20]
        positions = pd.DataFrame()
        positions['SecCode'] = ['A', 'B', 'C']
        positions['Ticker'] = ['A', 'B', 'C']
        positions['Shares'] = [10, 20, -50]
        result = make_orders(orders, positions)
        self.assertIsInstance(result[0], MOCOrder)
        self.assertEqual(result[0].quantity, 10)
        self.assertEqual(result[1].quantity, -17)
        self.assertEqual(result[2].quantity, 50)

    def test_write_size_containers(self):
        imp = StatArbImplementation(StatArbStrategyTest)
        imp.add_size_containers(self.size_containers)
        write_size_containers(imp, self.data_dir)
        #
        file_name = '{}_size_containers.json'.format(
            dt.date.today().strftime('%Y%m%d'))
        path = os.path.join(self.data_dir,
                            'StatArbStrategy',
                            'archive',
                            'size_containers')
        self.assertEqual(os.listdir(path)[0], file_name)

    def tearDown(self):
        ImplementationDataTestSuite().delete_data()


if __name__ == '__main__':
    unittest.main()
