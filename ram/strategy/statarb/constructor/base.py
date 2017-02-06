
from abc import ABCMeta, abstractmethod, abstractproperty


class BaseConstructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_daily_pl(self, **params):
        raise NotImplementedError('BaseConstructor.get_daily_pl')

    @abstractmethod
    def get_feature_names(self):
        raise NotImplementedError('BaseConstructor.get_feature_names')
