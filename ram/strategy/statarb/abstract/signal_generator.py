from abc import ABCMeta, abstractmethod, abstractproperty


class BaseSignalGenerator(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BaseSignalGenerator.get_args')

    @abstractmethod
    def set_data_args(self, data_container, **kargs):
        raise NotImplementedError('BaseSignalGenerator.set_data_args')

    # ~~~~~~ Model related functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def fit_model(self):
        raise NotImplementedError('BaseSignalGenerator.fit_model')

    @abstractmethod
    def get_model(self):
        raise NotImplementedError('BaseSignalGenerator.get_model')

    @abstractmethod
    def set_model(self):
        raise NotImplementedError('BaseSignalGenerator.set_model')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def get_signals(self):
        raise NotImplementedError('BaseSignalGenerator.get_signals')
