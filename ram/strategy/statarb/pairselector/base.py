
from abc import ABCMeta, abstractmethod, abstractproperty


class BasePairSelector(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_best_pairs(self, Close, OtherData, **params):
        """
        Parameters
        ----------
        Close/OtherData : data frame
            These are data frames that come out of bdh.make_data.
            The parameter name should correspond to the column name
            in the database. As of now, Close is hardcoded as the
            only column that is returned.
        params : params
            These are parameters that can be iterated over that
            are specific to this routine.

        RETURN Data Frame in the following columns:
            * Leg1/Leg2 : ids representing each side of a pair
        Option columns in data frame (these are fitted parameters for
            a specific pair):
            * window : how many periods to create z-score
            * z_enter/z_exit : parameters for trading

        Data Frame can also have trading parameters associated
        with it like window length and z-scores.
        """
        raise NotImplementedError('BaseStrategy.get_best_pairs')
