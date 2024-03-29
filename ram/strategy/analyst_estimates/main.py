
import itertools
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

class AnalystEstimates(Strategy):

    def strategy_init(self):
        # Set source code versions
        if self.strategy_code_version == 'version_001':
            from ram.strategy.analyst_estimates.version_001 import main
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

    def process_raw_data(self, data, time_index, market_data=None):
        self.data.add_data(data)

    def run_index(self, time_index):

        if len(self.data._processed_train_data) == 0:
            return
        elif len(self.data._processed_test_data) == 0:
            return
        elif self._write_flag and time_index < 16:
            return

        i = 0
        for ad in self._data_args:

            self.data.prep_data(**ad)

            for as_ in self._signals_args:

                self.signals.rf_signals(self.data, **as_)

                for ac in self._constructor_args:

                    result, stats = self.constructor.get_daily_pl(self.data,
                                                                  self.signals,
                                                                  **ac)
                    self._capture_output(result, stats, i)
                    i += 1

        self.write_index_results(self.output_returns, time_index)
        self.write_index_results(self.output_all_output, time_index,
                                 'all_output')
        self.write_index_stats(self.output_stats, time_index)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_column_parameters(self):
        """
        These are written to file
        """
        args1 = make_arg_iter(self.data.get_args())
        args2 = make_arg_iter(self.signals.get_args())
        args3 = make_arg_iter(self.constructor.get_args())

        output_params = {}
        for col_ind, (x, y, z) in enumerate(itertools.product(args1,
                                                              args2,
                                                              args3)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

    def _capture_output(self, results, stats, arg_index):
        returns = pd.DataFrame(results.PL / self.constructor.booksize)
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

    def implementation_training(self):
        # Import top params from wherever
        pass


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    from ram.strategy.base import StrategyVersionContainer
    from ram.data.data_constructor_blueprint import DataConstructorBlueprintContainer

    # Helpers
    from ram.strategy.analyst_estimates.version_001.data_blueprints import *
    blueprint_container = DataConstructorBlueprintContainer()
    blueprint_container.add_blueprint(univ1500_10)
    blueprint_container.add_blueprint(univ1500_15)

    strategy_versions = StrategyVersionContainer()
    strategy_versions.add_version('version_001', 'Current implementation')

    # Call Base Strategy Method
    from ram.strategy.base import make_argument_parser
    make_argument_parser(AnalystEstimates)
