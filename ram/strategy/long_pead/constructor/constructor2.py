import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.base_constructor import Constructor


class PortfolioConstructor2(Constructor):

    def get_args(self):
        return {
            'logistic_spread': [0.01, 0.1, 0.5, 1]
        }

    def get_position_sizes(self, scores, logistic_spread, n_groups):
        # Output should have all the same keys, but can return nan values
        output_sizes = {x: np.nan for x in scores.keys()}
        # Reformat input
        scores = pd.Series(scores, name='score').to_frame().join(
            pd.Series(self.market_cap, name='market_cap')).dropna()
        # Sort on market cap
        scores = scores.sort_values('market_cap')
        scores['group'] = np.arange(len(scores)) / n_groups
        # Get unique groups and their proportions. This is used for
        # total allocation to group.
        groups = scores.groupby('group')['score'].count()
        groups = groups / float(groups.sum())
        # Collect weighted output that has been scaled already.
        output = pd.DataFrame()
        for g, prop in groups.iteritems():
            output = output.append(weight_group(
                scores[scores.group == g], logistic_spread, prop))
        # Put data into output dictionary for downstream. Preserves nans
        for key in output_sizes.keys():
            output_sizes[key] = output.weights.loc[key]
        return output_sizes


def weight_group(data, logistic_spread, total_gross):
    def logistic_weight(k):
        return 2 / (1 + np.exp(-k)) - 1
    data = data.sort_values('score')
    data['weights'] = [
        logistic_weight(x) for x in np.linspace(
            -logistic_spread, logistic_spread, len(data))]
    data.weights = data.weights / data.weights.abs().sum() * total_gross
    return data
