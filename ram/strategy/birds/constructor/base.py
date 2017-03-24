from abc import ABCMeta, abstractmethod, abstractproperty

import numpy as np


class BaseConstructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_daily_pl(self, **params):
        raise NotImplementedError('BaseConstructor.get_daily_pl')

    def set_and_prep_data(self, data, signals):
        # Trim data
        self.close_dict = data.pivot(
            index='Date', columns='SecCode', values='RClose').T.to_dict()
        self.dividend_dict = data.pivot(
            index='Date', columns='SecCode',
            values='RCashDividend').fillna(0).T.to_dict()
        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        self.split_mult_dict = data.pivot(
            index='Date', columns='SecCode',
            values='SplitMultiplier').fillna(1).T.to_dict()
        # REFORMAT
        self.signals = signals
        #
        self.all_dates = np.unique(data.Date)
