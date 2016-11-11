import os
import shutil
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram.data.dh_sql import DataHandlerSQL


class Strategy(object):

    __metaclass__ = ABCMeta

    def __init__(self, output_dir=None):
        self._set_output_dir(output_dir)
        # Connect to QADirect
        self.datahandler = DataHandlerSQL()

    def start(self):
        """
        This should be the method implemented when running a strategy.
        """
        results = pd.DataFrame()

        for i in self.get_iter_index():
            temp_result = self.run_index(i)
            # Enforce that the index is DateTime
            assert isinstance(temp_result.index, pd.DatetimeIndex)
            results = results.add(temp_result, fill_value=0)

        self.results = results

    def run_index_writer(self, index):
        """
        This is a wrapper function for cloud implementation.
        """
        results = self.run_index(index)
        # Enforce that the index is DateTime
        assert isinstance(results.index, pd.DatetimeIndex)
        results.to_csv(self.output_dir+'/result_{0:05d}.csv'.format(index))

    def _set_output_dir(self, output_dir):
        if output_dir:
            # Output directory setup
            self.output_dir = os.path.join(output_dir, 'output_' +
                                           self.__class__.__name__)
            # Clean output directory if present
            if os.path.exists(self.output_dir):
                shutil.rmtree(self.output_dir)
            os.makedirs(self.output_dir)
        self.output_dir = output_dir

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