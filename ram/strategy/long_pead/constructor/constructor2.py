import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.constructor1 import PortfolioConstructor1


class PortfolioConstructor2(PortfolioConstructor1):

    def get_args(self):
        return {
            'logistic_spread': [0.01, 0.1, 0.5, 1]
        }

    def get_position_sizes(self, scores, logistic_spread, groups):

        scores = pd.Series(scores, name='score').to_frame().join(
            pd.Series(self.market_cap, name='market_cap'))

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
