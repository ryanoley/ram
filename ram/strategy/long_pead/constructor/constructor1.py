import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.base_constructor import Constructor


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'logistic_spread': [0.1],
            'losing_days_kill_switch': [4, 8, 1000],
            'rolling_return_days': [3, 6, None],
            'entry_threshold': [0.0010, 0.0020],
            'exit_threshold': [-0.0010, -0.0005]
        }

    def get_position_sizes(self,
                           scores, logistic_spread,
                           losing_days_kill_switch,
                           rolling_return_days,
                           entry_threshold,
                           exit_threshold):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """

        if not hasattr(self, 'booksize_original'):
            self.booksize_original = float(self.booksize)
            self.port_returns = []

        # Check losing days
        bad_seccodes = [x for x, y in self.portfolio.positions.iteritems()
                        if y.losing_day_count > losing_days_kill_switch]
        for sc in bad_seccodes:
            self.portfolio.positions[sc].close_position()
            scores[sc] = np.nan

        # Adjust booksize?
        if rolling_return_days:
            port_pl = sum([x.daily_pl for x in self.portfolio_tracker.positions.values()])
            self.port_returns.append(port_pl / float(self.booksize_original))
            if len(self.port_returns) > rolling_return_days:
                self.port_returns.pop(0)
            rolling_return = sum(self.port_returns) / rolling_return_days
            if (self.booksize != 0) & (rolling_return < exit_threshold):
                self.booksize = 0
            if (self.booksize == 0) & (rolling_return >= entry_threshold):
                self.booksize = float(self.booksize_original)

        # Get scores
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
        scores = scores.weights / scores.weights.abs().sum()
        scores_actual = (scores / scores.abs().sum() * self.booksize).to_dict()
        scores_tracker = (scores / scores.abs().sum() * self.booksize_original).to_dict()
        return scores_actual, scores_tracker
