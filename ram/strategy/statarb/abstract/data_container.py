from abc import ABCMeta, abstractmethod, abstractproperty


class BaseDataContainer(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BaseDataContainer.get_args')

    @abstractmethod
    def add_data(self, data, time_index):
        raise NotImplementedError('BaseDataContainer.add_data')

    @abstractmethod
    def prep_data(self, time_index, **kwargs):
        raise NotImplementedError('BaseDataContainer.prep_data')
