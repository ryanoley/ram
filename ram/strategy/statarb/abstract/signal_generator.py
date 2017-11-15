from abc import ABCMeta, abstractmethod, abstractproperty


class BaseSignal(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BaseSignal.get_args')

    @abstractmethod
    def get_skl_model(self):
        raise NotImplementedError('BaseSignal.get_skl_model')

    @abstractmethod
    def generate_signals(self, data_container, **kwargs):
        raise NotImplementedError('BaseSignal.generate_signals')
