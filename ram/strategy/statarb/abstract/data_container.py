from abc import ABCMeta, abstractmethod, abstractproperty


class BaseDataContainer(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BaseDataContainer.get_args')

    @abstractmethod
    def set_args(self, **kwargs):
        raise NotImplementedError('BaseDataContainer.set_args')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def get_train_data(self):
        raise NotImplementedError('BaseDataContainer.get_train_data')

    @abstractmethod
    def get_train_responses(self):
        raise NotImplementedError('BaseDataContainer.get_train_responses')

    @abstractmethod
    def get_train_features(self):
        raise NotImplementedError('BaseDataContainer.get_train_features')

    @abstractmethod
    def get_test_data(self):
        raise NotImplementedError('BaseDataContainer.get_test_data')

    @abstractmethod
    def get_constructor_data(self):
        raise NotImplementedError('BaseDataContainer.get_constructor_data')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def process_training_data(self, data, market_data, time_index):
        raise NotImplementedError('BaseDataContainer.process_training_data')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def prep_live_data(self, data, market_data):
        """
        This method can be used to do any pre-processing that should
        then be held in state. `process_live_data` is then called
        upon init of execution pipeline.
        """
        raise NotImplementedError('BaseDataContainer.prep_live_data')

    @abstractmethod
    def process_live_data(self, live_data):
        """
        Needs to return the properly formatted data frame to be
        sent along
        """
        raise NotImplementedError('BaseDataContainer.process_live_data')
