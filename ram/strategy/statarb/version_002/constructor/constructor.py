import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor


class PortfolioConstructor(BasePortfolioConstructor):

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

    def set_signals_constructor_data(self, signals, data):
        self._signals = signals.copy()
        self._scores = data['scores'].merge(signals)
        if 'pricing' in data:
            self._pricing = data['pricing']

    def get_day_position_sizes(self, scores, allocs):
        """
        For scores, Longs are low.
        For signals, Longs are high.
        """
        allocs = allocs.copy()

        scores = self._scores[['Signal', self._score_var]]
        scores = scores.sort_values('Signal')

        counts = scores.shape[0] / 2

        longs = scores.iloc[counts:]
        shorts = scores.iloc[:counts]

        longs = longs.sort_values('scores')
        shorts = shorts.sort_values('scores', ascending=False)

        longs = longs.iloc[:self._per_side_count]
        shorts = shorts.iloc[:self._per_side_count]

        counts = self._per_side_count * 2
        for s in longs.index.values:
            allocs[s] = 1 / float(counts) * BOOKSIZE

        for s in shorts.index.values:
            allocs[s] = -1 / float(counts) * BOOKSIZE

        return allocs
