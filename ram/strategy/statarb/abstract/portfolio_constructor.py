from abc import ABCMeta, abstractmethod, abstractproperty


class BasePortfolioConstructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BasePortfolioConstructor.get_args')

    @abstractmethod
    def get_daily_pl_data(self, data, time_index):
        raise NotImplementedError('BasePortfolioConstructor.add_data')

    @abstractmethod
    def get_position_sizes(self, data, time_index):
        raise NotImplementedError('BasePortfolioConstructor.add_data')
