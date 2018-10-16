import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor, BOOKSIZE

from ram.strategy.statarb.objects.sizes import SizeContainer


class PortfolioConstructor(BasePortfolioConstructor):

    def __init__(self):
        super(PortfolioConstructor, self).__init__()
        self._size_containers = {}

    def get_args(self):
        return {
            'score_var': ['prma_2_20',
                          'prma_2_40', 'prma_3_100',
                          'prma_3_20', 'prma_3_40',
                          'prma_40', 'prma_4_180', 'prma_80'],
            'per_side_count': [10],
            'holding_period': [3, 5]
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

    def set_signal_data(self, signals):
        self._signals = signals.copy()
        self._signals_scores = self._scores.merge(signals)

    def set_other_data(self, data):
        self._scores = data

    def get_day_position_sizes(self, date, column_index,
                               drop_short_seccodes=None):
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

        shorts = scores.iloc[:counts]
        longs = scores.iloc[counts:]

        if drop_short_seccodes:
            shorts = shorts[~shorts.index.isin(drop_short_seccodes)].copy()
            if len(shorts) < self._per_side_count:
                scores = shorts.append(longs)
                counts = scores.shape[0] / 2
                longs = scores.iloc[counts:]
                shorts = scores.iloc[:counts]

        longs = longs.sort_values(self._score_var)
        shorts = shorts.sort_values(self._score_var, ascending=False)

        longs = longs.iloc[:self._per_side_count]
        shorts = shorts.iloc[:self._per_side_count]

        counts = self._per_side_count * 2

        sizes = {}

        for i in longs.index.values:
            sizes[i] = 1 / float(counts) / self._holding_period

        for i in shorts.index.values:
            sizes[i] = -1 / float(counts) / self._holding_period

        size_container.update_sizes(sizes, date)

        return size_container.get_sizes()
