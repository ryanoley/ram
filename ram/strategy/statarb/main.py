import os
import sys
import imp
import json
import pickle
import inspect
import itertools
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.base import read_json
from ram.strategy.statarb import statarb_config

from ram.strategy.base import Strategy, StrategyVersionContainer
from ram.strategy.statarb.utils import make_arg_iter

from ram.strategy.statarb.data_blueprints import blueprint_container


# HELPER
strategy_versions = StrategyVersionContainer()

strategy_versions.add_version('version_002', 'Smart Factor')


class StatArbStrategy(Strategy):

    def strategy_init(self):
        # Set source code versions
        if self.strategy_code_version == 'version_002':
            from ram.strategy.statarb.version_002 import main
            self.data = main.data()
            self.signals = main.signals()
            self.constructor = main.constructor()

        else:
            print('Correct strategy code not specified')
            sys.exit()

        # Set args
        self._data_args = make_arg_iter(self.data.get_args())
        self._signals_args = make_arg_iter(self.signals.get_args())
        self._constructor_args = make_arg_iter(self.constructor.get_args())

    @staticmethod
    def get_data_blueprint_container():
        """
        Should return a dictionary with Blueprints in values and any
        labels as keys.
        """
        return blueprint_container

    @staticmethod
    def get_strategy_source_versions():
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

    def get_implementation_param_path(self):
        return os.path.join(os.getenv('GITHUB'), 'ram', 'ram',
                            'strategy', 'statarb', 'implementation',
                            'params', statarb_config.parameter_json)

    def process_raw_data(self, data, time_index, market_data=None):
        self.data.process_training_data(data, market_data, time_index)

    # ~~~~~~ Simulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run_index(self, time_index):
        # HACK: If training and writing, don't train until 2009, but stack data
        # Changed to 2003 for longer simulation
        if self._write_flag and \
                (int(self._prepped_data_files[time_index][:4]) < 2003):
            return

        column_index = 0
        for args1 in self._data_args:
            self.data.set_args(**args1)

            # Once data has been processed, pass relevant information
            # to portfolio constructor
            self.constructor.set_test_dates(self.data.get_test_dates())
            self.constructor.set_pricing_data(time_index,
                                              self.data.get_pricing_data())
            self.constructor.set_other_data(self.data.get_other_data())

            for args2 in self._signals_args:
                self.signals.set_args(**args2)
                self.signals.set_features(self.data.get_train_features())
                self.signals.set_train_data(self.data.get_train_data())
                self.signals.set_train_responses(
                    self.data.get_train_responses())
                self.signals.set_test_data(self.data.get_test_data())

                self.signals.fit_model()
                signals = self.signals.get_signals()

                # Pass signals to portfolio constructor
                self.constructor.set_signal_data(signals)

                for ac in self._constructor_args:
                    self.constructor.set_args(**ac)
                    result = self.constructor.get_period_daily_pl(column_index)
                    self._capture_output(result, column_index)
                    column_index += 1

        return self.output_returns, self.output_all_output, {}

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def process_implementation_params(self, run_name, params):
        # TODO: Hack to get proper training and test files for first part
        # of training
        master_train_data = self.data._processed_train_data.copy()
        master_test_data = self.data._processed_test_data.copy()
        master_test_dates = list(self.data._test_dates)
        master_pricing = self.data._pricing_data.copy()
        master_other = self.data._other_data.copy()

        self.data._processed_train_data = self.data._imp_train
        self.data._processed_test_data = self.data._imp_test
        self.data._test_dates = self.data._imp_test_dates
        self.data._pricing_data = self.data._imp_pricing
        self.data._other_data = self.data._imp_other

        self._process_implementation(params)

        # Get sizes and clean out containers
        del self.constructor._portfolios[0]
        sizes = self.constructor._size_containers.pop(0)

        output = {'params': params}
        output['sizes'] = sizes.to_json()

        # Retrain to get model
        self.data._processed_train_data = master_train_data
        self.data._processed_test_data = master_test_data
        self.data._test_dates = list(master_test_dates)
        self.data._pricing_data = master_pricing
        self.data._other_data = master_other

        self._process_implementation(params)

        del self.constructor._portfolios[0]
        del self.constructor._size_containers[0]

        self.implementation_training_write_params_model(
            run_name, output, self.signals.get_model())

    def _process_implementation(self, params):
        # Extract params
        data_params = dict([(k, params[k]) for k
                            in self.data.get_args().keys()])

        signal_params = dict([(k, params[k]) for k
                              in self.signals.get_args().keys()])

        constructor_params = dict([(k, params[k]) for k
                                   in self.constructor.get_args().keys()])

        # Fit data
        self.data.set_args(**data_params)

        self.signals.set_args(**signal_params)
        self.signals.set_features(self.data.get_train_features())
        self.signals.set_train_data(self.data.get_train_data())
        self.signals.set_train_responses(
            self.data.get_train_responses())
        self.signals.set_test_data(self.data.get_test_data())
        self.signals.fit_model()
        signals = self.signals.get_signals()

        self.constructor.set_test_dates(self.data.get_test_dates())
        self.constructor.set_pricing_data(0,
                                          self.data.get_pricing_data())
        self.constructor.set_other_data(self.data.get_other_data())

        self.constructor.set_signal_data(signals)

        self.constructor.set_args(**constructor_params)

        # This just needs to hold size containers which should then be
        # written out
        self.constructor.get_period_daily_pl(0)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, arg_index):
        results = results.copy()
        returns = pd.DataFrame(results.PL)
        returns.columns = [arg_index]
        # Rename columns
        results.columns = ['{}_{}'.format(x, arg_index)
                           for x in results.columns]
        if arg_index == 0:
            self.output_returns = returns
            self.output_all_output = results
        else:
            self.output_returns = self.output_returns.join(returns,
                                                           how='outer')
            self.output_all_output = self.output_all_output.join(
                results, how='outer')


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(StatArbStrategy)
