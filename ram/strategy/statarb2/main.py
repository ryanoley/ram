import os
import sys
import imp
import json
import pickle
import inspect
import itertools
import pandas as pd

from ram.strategy.base import Strategy, StrategyVersionContainer

from ram.strategy.statarb2.data_blueprints import blueprint_container

strategy_versions = StrategyVersionContainer()
strategy_versions.add_version('version_001', 'Factor Rank L/S')
strategy_versions.add_version('version_002', 'Dispersion')
strategy_versions.add_version('version_003', 'ML with training data')
strategy_versions.add_version('version_004', 'Double Sort')
strategy_versions.add_version('version_005', 'Pairs')
strategy_versions.add_version('version_006', 'Smart Factor')


class StatArbStrategy2(Strategy):

    def strategy_init(self):
        if self.strategy_code_version == 'version_001':
            from ram.strategy.statarb2.version_001.data import DataContainer
            from ram.strategy.statarb2.version_001.constructor import PortfolioConstructor
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
            self.signals = DummySignals()

        elif self.strategy_code_version == 'version_002':
            from ram.strategy.statarb2.version_002.data import DataContainer
            from ram.strategy.statarb2.version_002.constructor import PortfolioConstructor
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
            self.signals = DummySignals()

        elif self.strategy_code_version == 'version_003':
            from ram.strategy.statarb2.version_003.data import DataContainer
            from ram.strategy.statarb2.version_003.constructor import PortfolioConstructor
            from ram.strategy.statarb2.version_003.signals import SignalModel
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
            self.signals = SignalModel()

        elif self.strategy_code_version == 'version_004':
            from ram.strategy.statarb2.version_004.data import DataContainer
            from ram.strategy.statarb2.version_004.constructor import PortfolioConstructor
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
            self.signals = DummySignals()

        elif self.strategy_code_version == 'version_005':
            from ram.strategy.statarb2.version_005.data import DataContainer
            from ram.strategy.statarb2.version_005.constructor import PortfolioConstructor
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
            self.signals = DummySignals()

        elif self.strategy_code_version == 'version_006':
            from ram.strategy.statarb2.version_006.data import DataContainer
            from ram.strategy.statarb2.version_006.constructor import PortfolioConstructor
            from ram.strategy.statarb2.version_006.signals import SignalModel
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
            self.signals = SignalModel()

        else:
            print('Correct strategy code not specified')
            sys.exit()

    @staticmethod
    def get_data_blueprint_container():
        return blueprint_container

    @staticmethod
    def get_strategy_source_versions():
        return strategy_versions

    def get_column_parameters(self):
        output_params = {}
        for i, (x, y, z) in enumerate(
                itertools.product(
                    self.data.get_args(),
                    self.signals.get_args(),
                    self.constructor.get_args())):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[i] = params
        return output_params

    def process_raw_data(self, data, time_index, market_data=None):
        self.data.process_training_data(data, market_data, time_index)

    # ~~~~~~ Simulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run_index(self, time_index):

        if (time_index < 6):
            return

        if (time_index < 54) and self._write_flag:
            return

        i = 0

        for ad in self.data.get_args():

            self.data.set_args(**ad)

            for as_ in self.signals.get_args():
                self.signals.set_args(**as_)
                signals = self.signals.get_signals(self.data)

                for ac in self.constructor.get_args():
                    self.constructor.set_args(**ac)
                    results = self.constructor.process(self.data.trade_data,
                                                       signals)
                    self._capture_output(results, i)
                    i += 1

        self.write_index_results(self.output_returns, time_index)
        self.write_index_results(self.output_all_output,
                                 time_index,
                                 'all_output')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    def implementation_training(self):
        pass


class DummySignals(object):
    def get_args(self):
        return [{'v1': 10}]

    def set_args(self, **kwargs):
        pass



if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(StatArbStrategy2)
