from abc import ABCMeta, abstractmethod, abstractproperty


class BaseSignalGenerator(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BaseSignalGenerator.get_args')

    @abstractmethod
    def get_skl_model(self):
        raise NotImplementedError('BaseSignalGenerator.get_skl_model')

    @abstractmethod
    def generate_signals(self, data_container, **kwargs):
        raise NotImplementedError('BaseSignalGenerator.generate_signals')
