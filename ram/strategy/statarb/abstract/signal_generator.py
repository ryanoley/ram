from abc import ABCMeta, abstractmethod, abstractproperty


class BaseSignalGenerator(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BaseSignalGenerator.get_args')

    @abstractmethod
    def set_args(self, **kargs):
        raise NotImplementedError('BaseSignalGenerator.set_args')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def set_features(self, features):
        raise NotImplementedError('BaseSignalGenerator.set_features')

    @abstractmethod
    def set_train_data(self, train_data):
        raise NotImplementedError('BaseSignalGenerator.set_train_data')

    @abstractmethod
    def set_train_responses(self, train_responses):
        raise NotImplementedError('BaseSignalGenerator.set_train_responses')

    @abstractmethod
    def set_test_data(self, test_data):
        raise NotImplementedError('BaseSignalGenerator.set_test_data')

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
