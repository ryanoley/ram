import os
import sys
import imp
import json
import pickle
import inspect
import itertools
import pandas as pd
import datetime as dt
from sklearn.externals import joblib

from ram import config
from ram.strategy.base import Strategy, StrategyVersionContainer
from ram.strategy.statarb.utils import make_arg_iter

from ram.strategy.statarb.data_blueprints import blueprint_container

# HELPER
strategy_versions = StrategyVersionContainer()
strategy_versions.add_version('version_001', 'Current implementation')


class StatArbStrategy(Strategy):

    def strategy_init(self):
        # Set source code versions
        if self.strategy_code_version == 'version_001':
            from ram.strategy.statarb.version_001 import main
            self.data = main.data
            self.signals = main.signals
            self.constructor = main.constructor
        else:
            print('Correct strategy code not specified')
            sys.exit()
        # Set args
        self._data_args = make_arg_iter(self.data.get_args())
        self._signals_args = make_arg_iter(self.signals.get_args())
        self._constructor_args = make_arg_iter(self.constructor.get_args())

    def get_data_blueprint_container(self):
        """
        Should return a dictionary with Blueprints in values and any
        labels as keys.
        """
        return blueprint_container

    def get_strategy_source_versions(self):
        """
        Should return a dictionary with descriptions in values and any
        labels as keys.
        """
        return strategy_versions

    def get_column_parameters(self):
        output_params = {}
        for col_ind, (x, y, z) in enumerate(itertools.product(
                self._data_args, self._signals_args, self._constructor_args)):
            params = {}
            params['data'] = x
            params['signals'] = y
            params['constructor'] = z
            output_params[col_ind] = params
        return output_params

    def process_raw_data(self, data, time_index, market_data=None):
        self.data.add_market_data(market_data)
        self.data.add_data(data, time_index)

    def run_index(self, index):
        # HACK: If training and writing, don't train until 2007, but stack data
        if self._write_flag and \
                (int(self._prepped_data_files[time_index][:4]) < 2007):
            return
        # Iterate
        i = 0
        for ad in self._data_args:
            self.data.prep_data(time_index, **ad)

            for as_ in self._signals_args:
                self.signals.generate_signals(self.data, **as_)

                for ac in self._constructor_args:
                    result, stats = self.constructor.get_daily_pl(
                        self.data, self.signals, **ac)
                    self._capture_output(result, stats, i)
                    i += 1

        self.write_index_results(self.output_returns, time_index)
        self.write_index_results(self.output_all_output,
                                 time_index,
                                 'all_output')
        self.write_index_stats(self.output_stats, time_index)

    # ~~~~~~ Implementation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # def implementation_training(self, cli_input):
    #     """
    #     As of now this is a hard-coded implementation!
    #     """
    #     combo_name, run_name = cli_input[0], cli_input[1]
    #     # Load combos
    #     if self._gcp_implementation:
    #         path = os.path.join('combo_search',
    #                             combo_name, 'current_top_params.json')
    #         blob = self._bucket.get_blob(path)
    #         params = json.loads(blob.download_as_string())
    #     else:
    #         path = os.path.join(config.COMBO_SEARCH_OUTPUT_DIR,
    #                             combo_name, 'current_top_params.json')
    #         params = json.load(open(path, 'r'))
    #     # Check if run is needed
    #     column_params = self._check_run_get_params(run_name, params)
    #     # Stack needed data
    #     self._implementation_training_stack_run_data(run_name)
    #     # With unique runs, import data, import
    #     for key, cparams in column_params.iteritems():
    #         data_params = self._get_data_params(cparams)
    #         signals_params = self._get_signals_params(cparams)
    #         time_index = len(self._prepped_data_files) - 1
    #         time_index = 1  ## TEMP
    #         self.data.prep_data(time_index, **data_params)
    #         self.signals.generate_signals(self.data, **signals_params)
    #         model = self.signals.get_skl_model()
    #         # Cache model
    #         if self._gcp_implementation:
    #             model_cache_path = os.path.join(
    #                 'combo_search', combo_name, key+'.pkl')
    #             blob = self._bucket.blob(model_cache_path)
    #             blob.upload_from_string(pickle.dumps(model))
    #         else:
    #             model_cache_path = os.path.join(
    #                 config.COMBO_SEARCH_OUTPUT_DIR,
    #                 combo_name, key+'.pkl')
    #             joblib.dump(model, model_cache_path)

    # def _check_run_get_params(self, run_name, params):
    #     # Process runs
    #     run_map = {}
    #     for key in params.keys():
    #         key2 = key.split('_')[1] + '_' + key.split('_')[2]
    #         if key2 not in run_map:
    #             run_map[key2] = []
    #         run_map[key2].append(key)
    #     if run_name not in run_map:
    #         sys.exit()
    #     column_params = {}
    #     for key in run_map[run_name]:
    #         column_params[key] = params[key]
    #     return column_params

    def _get_data_params(self, params):
        params = params['column_params']
        interface = inspect.getargspec(self.data.prep_data).args
        interface = [x for x in interface if x not in ['self', 'time_index']]
        out = {}
        for key in interface:
            out[key] = params[key]
        return out

    def _get_signals_params(self, params):
        params = params['column_params']
        interface = inspect.getargspec(self.signals.generate_signals).args
        interface = [x for x in interface
                     if x not in ['self', 'data_container']]
        out = {}
        for key in interface:
            out[key] = params[key]
        return out

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, stats, arg_index):
        results = results.copy()
        if hasattr(self.constructor, 'booksize_original'):
            book = self.constructor.booksize_original
        else:
            book = self.constructor.booksize
        returns = pd.DataFrame(results.PL / book)
        returns.columns = [arg_index]
        # Rename columns
        results.columns = ['{}_{}'.format(x, arg_index)
                           for x in results.columns]
        if arg_index == 0:
            self.output_returns = returns
            self.output_all_output = results
            self.output_stats = {}
        else:
            self.output_returns = self.output_returns.join(returns,
                                                           how='outer')
            self.output_all_output = self.output_all_output.join(
                results, how='outer')
        self.output_stats[arg_index] = stats

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(StatArbStrategy)
