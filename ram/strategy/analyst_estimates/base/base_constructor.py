import numpy as np
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty


class Constructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_position_sizes(self):
        raise NotImplementedError('Constructor.get_position_sizes')

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('Constructor.get_args')

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize

    def _get_position_sizes_dollars(self, sizes):
        """
        Setup to normalize outputs from derived class. Uses booksize
        to convert to dollars
        """
        if isinstance(sizes, dict):
            sizes = pd.Series(sizes)
        return (sizes * self.booksize).to_dict()


def filter_seccodes(data_dict, min_value):
    bad_seccodes = []
    for key, value in data_dict.iteritems():
        if value < min_value:
            bad_seccodes.append(key)
    return bad_seccodes
