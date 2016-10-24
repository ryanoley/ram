from abc import ABCMeta, abstractmethod, abstractproperty

from ram.data.base import DataHandler


class Strategy(object):

    __metaclass__ = ABCMeta

    def attach_data_source(self, data):
        assert isinstance(data, DataHandler)
        self.data = data

    def start(self):
        """
        Iterates through time and emits results to reporting classes.
        """
        pass

    @abstractmethod
    def get_result(self):
        raise NotImplementedError('Strategy.get_result')

    @abstractmethod
    def run_iteration(self, t):
        raise NotImplementedError('Strategy.run_iteration')
