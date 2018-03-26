import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor, BOOKSIZE

from ram.strategy.statarb.version_002.constructor.sizes import SizeContainer


class PortfolioConstructor(BasePortfolioConstructor):

    _size_containers = {}

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
        For signals, Longs are high. (Signal is from sklearn model)
        For scores, Longs are low. (Score is technical var)
        """
        if column_index in self._size_containers:
            size_container = self._size_containers[column_index]

        else:
            size_container = SizeContainer(self._holding_period)
            self._size_containers[column_index] = size_container

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
            allocs[i] = 1 / float(counts) * BOOKSIZE / self._holding_period

        for i in shorts.index.values:
            allocs[i] = -1 / float(counts) * BOOKSIZE / self._holding_period
        size_container.update_sizes(allocs)

        return size_container.get_sizes()
