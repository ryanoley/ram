import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.base_constructor import Constructor


class PortfolioConstructorPairs(Constructor):

    def get_args(self):
        return {
            'logistic_spread': [0.1],
            'losing_days_kill_switch': [1000],
            'zscore_thresh': [1.2, 2]
        }

    def get_position_sizes(self, date, scores,
                           logistic_spread,
                           losing_days_kill_switch,
                           zscore_thresh):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        all_seccodes = scores.keys()
        # Check losing days
        bad_seccodes = [x for x, y in self.portfolio.positions.iteritems()
                        if y.losing_day_count > losing_days_kill_switch]
        for sc in bad_seccodes:
            self.portfolio.positions[sc].close_position()
            scores[sc] = np.nan
        # Get scores
        zscores = pd.DataFrame({'zscore': self.data_container.zscores.loc[date]})
        zscores = zscores.reset_index()
        zscores.columns = ['Pair', 'zscore']
        zscores = zscores.merge(self.data_container.zscores_leg_map)
        zscores = zscores.drop(['Pair'], axis=1)

        scores = pd.Series(scores).to_frame().dropna()
        scores = scores.reset_index()
        scores.columns = ['Leg1', 'Score1']
        zscores = zscores.merge(scores)
        scores.columns = ['Leg2', 'Score2']
        zscores = zscores.merge(scores)

        scores = _get_sizes(zscores, zscore_thresh)
        scores = pd.Series(0, index=all_seccodes).add(scores)
        scores.name = 'score'
        scores = scores.to_frame()
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
        scores.weights *= self.booksize
        return scores.weights.to_dict()


def _get_sizes(data, zscore_thresh):

    temp1 = data[['Leg1', 'Leg2', 'Score1', 'Score2', 'zscore', 'distances']].copy()
    temp2 = temp1.copy()
    temp2.zscore *= -1
    temp2.columns = ['Leg2', 'Leg1', 'Score2', 'Score1', 'zscore', 'distances']
    data = temp1.append(temp2)

    temp1 = data[(data.Score1 < 0) & (data.zscore > zscore_thresh)]
    temp2 = data[(data.Score1 > 0) & (data.zscore < -zscore_thresh)]
    data = temp1.append(temp2)
    norm_factor = data.groupby('Leg1')['zscore'].sum().reset_index()

    norm_factor.columns = ['Leg1', 'norm_factor']
    data = data.merge(norm_factor)
    data['offset_size'] = data.zscore / data.norm_factor * data.Score1 * -1

    offset_size = data.groupby('Leg2')['offset_size'].sum()
    main_size = data.groupby('Leg1')['Score1'].max()

    return main_size.add(offset_size, fill_value=0)
