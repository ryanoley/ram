from abc import ABCMeta, abstractmethod, abstractproperty


class BaseStatArbStrategy(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def generate_signals(self, data_container, **kwargs):
        raise NotImplementedError('BaseStatArbStrategy.generate_signals')
