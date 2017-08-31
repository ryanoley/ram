import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.basic.constructor.portfolio import Portfolio
from ram.strategy.basic.constructor.base_constructor import Constructor


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'logistic_spread': [0.1, 0.5, 1.0, 5]
        }

    def get_position_sizes(self, scores, logistic_spread):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        scores = pd.Series(scores).to_frame()
        scores.columns = ['score']
        scores = scores.sort_values('score')

        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1

        n_good = (~scores.score.isnull()).sum()
        n_bad = scores.score.isnull().sum()
        scores['weights'] = [
            logistic_weight(x) for x in np.linspace(
                -logistic_spread, logistic_spread, n_good)] + [0] * n_bad
        scores.weights = scores.weights / scores.weights.abs().sum()
        return scores.weights.to_dict()
