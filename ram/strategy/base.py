from abc import ABCMeta, abstractmethod, abstractproperty


class Strategy(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        """
        Will be used to call method in batch updates.
        """
        raise NotImplementedError('Strategy.start')

    @abstractmethod
    def get_results(self):
        raise NotImplementedError('Strategy.get_results')
