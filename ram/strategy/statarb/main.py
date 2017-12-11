import os
import sys
import imp
import json
import pickle
import inspect
import itertools
import pandas as pd
import datetime as dt
from copy import deepcopy

from ram import config
from ram.strategy.statarb import statarb_config

from ram.strategy.base import Strategy, StrategyVersionContainer
from ram.strategy.statarb.utils import make_arg_iter

from ram.strategy.statarb.data_blueprints import blueprint_container

from ram.strategy.statarb.implementation.preprocess_new_models import \
    import_current_top_params

# HELPER
strategy_versions = StrategyVersionContainer()
strategy_versions.add_version('version_001', 'Current implementation')


class StatArbStrategy(Strategy):

    def strategy_init(self):
        # Set source code versions
        if self.strategy_code_version == 'version_001':
            from ram.strategy.statarb.version_001 import main
            self.data = deepcopy(main.data)
            self.signals = deepcopy(main.signals)
            self.constructor = deepcopy(main.constructor)
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
                self._data_args,
                self._signals_args,
                self._constructor_args)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

    def process_raw_data(self, data, time_index, market_data=None):
        self.data.process_training_market_data(market_data)
        self.data.process_training_data(data, time_index)

    # ~~~~~~ Simulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run_index(self, time_index):
        # HACK: If training and writing, don't train until 2009, but stack data
        if self._write_flag and \
                (int(self._prepped_data_files[time_index][:4]) < 2009):
            return

        # Iterate
        i = 0
        for args1 in self._data_args:

            self.data.set_args(**args1)

            for args2 in self._signals_args:

                self.signals.set_args(**args2)
                self.signals.set_features(self.data.get_train_features())
                self.signals.set_train_data(self.data.get_train_data())
                self.signals.set_train_responses(
                    self.data.get_train_responses())
                self.signals.set_test_data(self.data.get_test_data())

                self.signals.fit_model()
                signals = self.signals.get_signals()

                # HACK
                # Make sure that there is no dependence on variables
                self.constructor.set_signals_constructor_data(
                    signals, self.data.get_constructor_data())

                for ac in self._constructor_args:

                    self.constructor.set_args(**ac)

                    result, stats = self.constructor.get_period_daily_pl()

                    self._capture_output(result, stats, i)
                    i += 1

        self.write_index_results(self.output_returns, time_index)
        self.write_index_results(self.output_all_output,
                                 time_index,
                                 'all_output')
        self.write_index_stats(self.output_stats, time_index)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def implementation_training(self):
        # Import top params from wherever
        top_params = import_current_top_params().keys()

        # Process
        run_map = self.implementation_training_prep(top_params)
        # Placeholder to determine if data should be reloaded
        current_stack_index = None
        for i, vals in run_map.iterrows():
            if vals.stack_index != current_stack_index:
                self.strategy_code_version = vals.strategy_version
                self.prepped_data_version = vals.data_version
                current_stack_index = vals.stack_index
                print('[[Stacking data]]')
                self.strategy_init()
                self.implementation_training_stack_version_data(
                    vals.data_version)
                all_params = self.import_run_column_params(
                    vals.run_name)
            params = all_params[vals.column_name]
            # Fit model and cache
            data_params = dict([(k, params[k]) for k
                                in self.data.get_args().keys()])
            self.data.set_args(**data_params)
            signal_params = dict([(k, params[k]) for k
                                  in self.signals.get_args().keys()])
            self.signals.set_args(**signal_params)
            self.signals.set_features(self.data.get_train_features())
            self.signals.set_train_data(self.data.get_train_data())
            self.signals.set_train_responses(
                self.data.get_train_responses())
            self.signals.set_test_data(self.data.get_test_data())
            self.signals.fit_model()
            self.implementation_training_write_params_model(
                vals.param_name, params, self.signals.get_model())

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, stats, arg_index):
        results = results.copy()
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


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(StatArbStrategy)
