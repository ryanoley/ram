from abc import ABCMeta, abstractmethod, abstractproperty


class CommittedStrategy(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_meta_data(self):
        raise NotImplementedError('CommittedStrategy.get_meta_data')

    @abstractmethod
    def get_daily_returns(self):
        raise NotImplementedError('CommittedStrategy.get_daily_returns')
