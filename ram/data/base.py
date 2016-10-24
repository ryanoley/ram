from abc import ABCMeta, abstractmethod, abstractproperty


class DataHandler(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_data(self):
        raise NotImplementedError('DataHandler.get_data')
