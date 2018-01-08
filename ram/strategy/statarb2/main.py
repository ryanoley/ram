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
strategy_versions.add_version('version_001', 'Current implementation')


class StatArbStrategy2(Strategy):

    def strategy_init(self):
        if self.strategy_code_version == 'version_001':
            from ram.strategy.statarb2.version_001.data import DataContainer
            from ram.strategy.statarb2.version_001.constructor import PortfolioConstructor
            self.data = DataContainer()
            self.constructor = PortfolioConstructor()
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
        for i, (x, y) in enumerate(
                itertools.product(
                    self.data.get_args(),
                    self.constructor.get_args())):
            params = dict(x)
            params.update(y)
            output_params[i] = params
        return output_params

    def process_raw_data(self, data, time_index, market_data=None):
        self.data.process_training_data(data, market_data, time_index)

    # ~~~~~~ Simulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run_index(self, time_index):
        import pdb; pdb.set_trace()
        for ad in self.data.get_args():
            self.data.set_args(**ad)
            for ac in self.constructor.get_args():
                self.constructor.set_args(**ac)
                results = self.constructor.process(self.data.train_data,
                                                   self.data.test_data)
                # TODO: CAPTURE
        self.write_index_results(rets, time_index)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def implementation_training(self):
        pass


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(StatArbStrategy2)
