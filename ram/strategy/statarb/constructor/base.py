from abc import ABCMeta, abstractmethod, abstractproperty

import numpy as np


class BaseConstructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_iterable_args(self, data, cut_date):
        raise NotImplementedError('BaseConstructor.get_iterable_args')

    @abstractmethod
    def get_daily_pl(self, **params):
        raise NotImplementedError('BaseConstructor.get_daily_pl')

    @abstractmethod
    def get_feature_names(self):
        raise NotImplementedError('BaseConstructor.get_feature_names')

    def set_and_prep_data(self, scores, pair_info, data):
        # Trim data
        data = data[data.Date.isin(scores.index)].copy()
        self.data = data

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

        # Need all scores for exit
        self.exit_scores = scores.T.to_dict()

        scores = scores.unstack().reset_index()
        scores.columns = ['Pair', 'Date', 'score']
        scores['abs_score'] = scores.score.abs()
        scores['side'] = np.where(scores.score <= 0, 1, -1)
        scores = scores.sort_values(['Date', 'abs_score'], ascending=False)
        scores['deliverable'] = zip(scores.abs_score, scores.Pair, scores.side)
        self.enter_scores = {}
        for d in np.unique(self.exit_scores.keys()):
            self.enter_scores[d] = scores.deliverable[scores.Date == d].values

        self.all_dates = np.unique(self.exit_scores.keys())
        self.pair_info = pair_info
