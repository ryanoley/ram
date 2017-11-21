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
    def process_training_data(self, data, time_index):
        raise NotImplementedError('BaseDataContainer.process_training_data')

    @abstractmethod
    def process_training_market_data(self, data):
        raise NotImplementedError('BaseDataContainer.process_training_market')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def get_training_data(self):
        """
        Used by signal class
        """
        raise NotImplementedError('BaseDataContainer.get_training_data')

    @abstractmethod
    def get_training_responses(self):
        """
        Used by signal class
        """
        raise NotImplementedError('BaseDataContainer.get_training_responses')

    @abstractmethod
    def get_training_feature_names(self):
        """
        Used by signal class
        """
        raise NotImplementedError('BaseDataContainer.get_training_features')

    @abstractmethod
    def get_test_data(self):
        """
        Used by signal class
        """
        raise NotImplementedError('BaseDataContainer.get_test_data')

    @abstractmethod
    def get_simulation_feature_dictionary(self):
        """
        Used by constructor class. Can have any deliverable in it
        """
        raise NotImplementedError('BaseDataContainer.get_simulation_features')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def prep_live_data(self, data):
        """
        This method can be used to do any pre-processing that should
        then be held in state. `process_live_data` is then called
        upon init of execution pipeline.
        """
        raise NotImplementedError('BaseDataContainer.prep_live_data')

    @abstractmethod
    def process_live_data(self, data):
        """
        Needs to return the properly formatted data frame to be
        sent along
        """
        raise NotImplementedError('BaseDataContainer.process_live_data')
