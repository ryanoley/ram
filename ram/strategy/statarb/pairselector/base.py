
from abc import ABCMeta, abstractmethod, abstractproperty


class BasePairSelector(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_best_pairs(self, data, cut_date):
        """
        Returns
        -------
        scores : pd.DataFrame
            A data frame with dates in the index, pairs in the columns,
            and some score to sort on downstream.
        pair_info : pd.DataFrame
            A data frame with Leg1 and Leg2 columns, and then data
            that could be used with them downstream for portfolio construction
            like Sector.
        """
        raise NotImplementedError('BaseStrategy.get_best_pairs')

    @abstractmethod
    def get_feature_names(self):
        raise NotImplementedError('BaseStrategy.get_feature_names')
