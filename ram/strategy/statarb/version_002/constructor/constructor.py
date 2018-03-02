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

    def V1get_day_position_sizes(self, date, scores):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        # Capture all seccodes for output object. Bad SecCodes will be filtered
        # during construction phase.

        all_seccodes = scores.keys()
        clean_scores = {x: y for x, y in scores.iteritems() if ~np.isnan(y)}

        zscores = self._zscores.loc[date]

        # Only keep zscores that have signals, because nans represent
        # stocks that have been filtered
        zscores = zscores[
            (zscores.SecCode.isin(clean_scores.keys())) |
            (zscores.OffsetSecCode.isin(clean_scores.keys()))]

        # Get relative position sizes
        scores = _select_port_and_offsets(clean_scores, zscores, self._params)

        # Scale to dollar value
        for key in scores.keys():
            scores[key] *= self.booksize
        # Add in missing scores, and return dictionary
        missing_codes = list(set(all_seccodes) -
                             set(scores.keys()))
        scores2 = dict(zip(missing_codes, [0]*len(missing_codes)))
        scores.update(scores2)

        return scores

    def get_day_position_sizes(self, date, scores):
        import pdb; pdb.set_trace()
        x = 10

    def KEEPTHISget_day_position_sizes(self, scores, allocs):
        """
        For scores, Longs are low.
        For signals, Longs are high.
        """
        allocs = allocs.copy()

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



