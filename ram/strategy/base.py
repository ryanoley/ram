import os
import json
import shutil
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram.data.dh_sql import DataHandlerSQL
from gearbox import ProgBar


class Strategy(object):

    __metaclass__ = ABCMeta

    def __init__(self, output_dir=None, run_version=None):
        self._set_output_dir(output_dir, run_version)
        # Connect to QADirect
        self.datahandler = DataHandlerSQL()

    def start(self):
        """
        This should be the method implemented when running a strategy.
        """
        try:
            # Objects that can be returned by run_index
            returns = pd.DataFrame()
            column_params = {}
            statistics = {}

            for i in ProgBar(self.get_iter_index()):
                output = self.run_index(i)
                if isinstance(output, dict):
                    rets = output['returns']
                elif isinstance(output, pd.DataFrame):
                    rets = output
                elif output is None:
                    # For DEBUGGING
                    continue
                # Enforce that the index is DateTime
                assert isinstance(rets.index, pd.DatetimeIndex)
                returns = returns.add(rets, fill_value=0)
                if 'statistics' in output:
                    statistics[i] = output['statistics']

            self.returns = returns
            if 'column_params' in output:
                self.column_params = output['column_params']
            else:
                self.column_params = None
            if len(statistics) > 0:
                self.statistics = statistics
            else:
                self.statistics = None

            self._write_results()

        except KeyboardInterrupt:
            print '\nClosing Database Connection'
            self.datahandler.close_connections()
            raise KeyboardInterrupt

        self.datahandler.close_connections()

    def run_index_writer(self, index):
        """
        This is a wrapper function for cloud implementation.
        """
        results = self.run_index(index)
        if isinstance(results, dict):
            results = results['returns']
        # Enforce that the index is DateTime
        assert isinstance(results.index, pd.DatetimeIndex)
        results.to_csv(self.strategy_output_dir+'/result_{0:05d}.csv'.format(index))

    def _set_output_dir(self, output_dir, run_version):
        self.strategy_output_dir = None
        if output_dir:
            if not run_version:
                assert 'Provide strategy run_version to Strategy.init'
            # Make directory for StrategyClass
            ddir = os.path.join(output_dir, self.__class__.__name__)
            if not os.path.exists(output_dir):
                os.makedirs(ddir)
            # Output directory setup
            self.strategy_output_dir = os.path.join(ddir, run_version)
            # Clean output directory if present
            if os.path.exists(self.strategy_output_dir):
                shutil.rmtree(self.strategy_output_dir)
            os.makedirs(self.strategy_output_dir)

    def _write_results(self):
        if self.strategy_output_dir:
            self.returns.to_csv(os.path.join(self.strategy_output_dir,
                                             'results.csv'))
            if self.column_params:
                with open(os.path.join(self.strategy_output_dir,
                                       'params.json'), 'w') as outfile:
                    json.dump(self.column_params, outfile)
                outfile.close()

            if self.statistics:
                with open(os.path.join(self.strategy_output_dir,
                                       'statistics.json'), 'w') as outfile:
                    json.dump(self.statistics, outfile)
                outfile.close()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def run_index(self):
        """
        Takes in integer
        """
        raise NotImplementedError('Strategy.run_index')

    @abstractmethod
    def get_iter_index(self):
        """
        Returns list of integers that will be run by run_index
        """
        raise NotImplementedError('Strategy.get_iter_index')
