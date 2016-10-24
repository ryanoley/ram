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
        """
        If there was some parallelization, this can allow for some
        formatting.
        """
        raise NotImplementedError('Strategy.get_results')

    @abstractmethod
    def start_live(self):
        """
        This method will be called overnight to track the live
        trading of all systems.
        """
        raise NotImplementedError('Strategy.start_live')
