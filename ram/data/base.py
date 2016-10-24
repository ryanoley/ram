from abc import ABCMeta, abstractmethod, abstractproperty


class DataHandler(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_filtered_univ_data(self):
        raise NotImplementedError('DataHandler.get_filtered_univ_data')

    @abstractmethod
    def get_id_data(self):
        raise NotImplementedError('DataHandler.get_id_data')
