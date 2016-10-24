from abc import ABCMeta, abstractmethod, abstractproperty

from platform.data.base import DataHandler


class Strategy(object):

    __metaclass__ = ABCMeta

    def run_iteration(self, t):
        pass

    def attach_data(self, data):
        assert isinstance(data, DataHandler)
        self.data = data

    @abstractmethod
    def get_result(self):
        raise NotImplementedError('BaseStrategy.get_result')
