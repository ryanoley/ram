import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor, BOOKSIZE


class PortfolioConstructor(BasePortfolioConstructor):

    _sizes = {}

    def get_args(self):
        return {
            'score_var': ['prma_10', 'prma_15', 'prma_20', 'ret_10d',
                          'boll_10', 'boll_20', 'rsi_15'],
            'per_side_count': [10, 20, 30],
            'holding_period': [3, 5, 7, 9]
        }

    def set_args(self,
                 score_var,
                 per_side_count,
                 holding_period):
        self._score_var = score_var
        self._per_side_count = per_side_count
        self._holding_period = holding_period
        # Format scores data
        scores2 = self._signals_scores[['Date', 'SecCode',
                                        'Signal', score_var]].copy()
        scores2 = scores2.sort_values(['Date', 'Signal', score_var])
        scores2 = scores2.set_index(['Date', 'SecCode'])
        self._signals_scores2 = scores2

    def set_signal_data(self, time_index, signals):
        self._signals = signals.copy()
        self._signals_scores = self._scores.merge(signals)

    def set_other_data(self, time_index, data):
        self._scores = data

    def get_day_position_sizes(self, date, column_index):
        """
        For scores, Longs are low.
        For signals, Longs are high.
        """
        if column_index in self._sizes:
            sizes = self._sizes[column_index]
        else:
            sizes = SizeContainer(self._holding_period)
            self._sizes[column_index] = sizes

        scores = self._signals_scores2.loc[date].copy()

        counts = scores.shape[0] / 2

        longs = scores.iloc[counts:]
        shorts = scores.iloc[:counts]

        longs = longs.sort_values(self._score_var)
        shorts = shorts.sort_values(self._score_var, ascending=False)

        longs = longs.iloc[:self._per_side_count]
        shorts = shorts.iloc[:self._per_side_count]

        counts = self._per_side_count * 2

        allocs = {}
        for i in longs.index.values:
            allocs[i] = 1 / float(counts) * BOOKSIZE

        for i in shorts.index.values:
            allocs[i] = -1 / float(counts) * BOOKSIZE
        sizes.update_sizes(allocs)

        return sizes.get_sizes()


class SizeContainer(object):

    def __init__(self, n_days):
        self.n_days = n_days
        self.sizes = {}
        self.index = 0

    def update_sizes(self, sizes):
        self.sizes[self.index] = sizes
        # Clean out old
        if (self.index - self.n_days) in self.sizes:
            del self.sizes[self.index - self.n_days]
        self.index += 1

    def get_sizes(self):
        # Init output with all seccods
        output = {x: 0 for x in set(sum([x.keys() for x
                                         in self.sizes.values()], []))}

        for i in self.sizes.keys():
            for j in self.sizes[i].keys():
                output[j] += self.sizes[i][j] / float(self.n_days)

        return output
