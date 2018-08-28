import numpy as np
import pandas as pd
import itertools as it
import datetime as dt

from ram.analysis.model_selection.model_selection import ModelSelection


class ScoreVariables(ModelSelection):

    def get_implementation_name(self):
        return 'ScoreVariables'

    # For randomly generated integers
    n_best_score_vars = 6
    n_best_per_var = 2  # Not implemented
    strats_per_port = 5

    def set_selection_criteria(self, criteria='mean'):
        self.criteria = criteria

    def get_top_models(self, time_index, train_data):
        """
        Main selection mechanism of top combinations. Optimizes on Sharpe.
        """
        # Get all score_vars and the indexes

        # Map column name to index
        col_name_map = {c: i for i, c in enumerate(self._raw_returns.columns)}

        # Group score vars
        temp = [(v['column_params']['score_var'], k) for k, v in self._raw_column_params.iteritems()]
        score_var_map = {}
        for t in temp:
            if t[0] not in score_var_map:
                score_var_map[t[0]] = []
            score_var_map[t[0]].append(col_name_map[t[1]])

        if not hasattr(self, 'criteria'):
            self.set_selection_criteria()

        if self.criteria == 'sharpe':
            scores = train_data.mean() / train_data.std()

        elif self.criteria == 'mean':
            scores = train_data.mean()

        scores.index = range(len(scores))

        score_hold = []

        for k, v in score_var_map.iteritems():
            i = scores.iloc[v].idxmax()
            s = scores.loc[i]
            score_hold.append((s, i))

        score_hold.sort()

        best_inds = [x[1] for x in score_hold[-self.n_best_score_vars:]]
        best_scores = [x[0] for x in score_hold[-self.n_best_score_vars:]]

        return [best_inds], [sum(best_scores)]
