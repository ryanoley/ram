import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.base_constructor import Constructor


class PortfolioConstructor2(Constructor):

    def get_args(self):
        return {
            'logistic_spread': [0.1, 2],
            'group_variable': ['MarketCap', 'Liquidity', 'Sector']
        }

    def get_position_sizes(self, date, scores, logistic_spread,
                           group_variable, n_groups=3):
        # Output should have all the same keys, but can return nan values
        output_sizes = pd.DataFrame(index=scores.keys(),
                                    columns=['weight'])
        # Reformat input
        if group_variable == 'MarketCap':
            scores = pd.Series(scores, name='score').to_frame().join(
                pd.Series(self.market_cap, name='sort_var')).dropna()
        elif group_variable == 'Liquidity':
            scores = pd.Series(scores, name='score').to_frame().join(
                pd.Series(self.liquidity, name='sort_var')).dropna()
        else:
            scores = pd.Series(scores, name='score').to_frame().join(
                pd.Series(self.sector, name='group')).dropna()

        if group_variable == 'Sector':
            # Drop anything that isn't real gics sector
            scores = scores.loc[scores.group.isin(
                [str(x) for x in range(10, 60, 5)])]
        else:
            # Sort on market cap
            scores = scores.sort_values('sort_var')
            scores['group'] = np.floor(
                np.arange(len(scores)) / float(
                    len(scores)) * n_groups).astype(int)

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
        output_sizes.loc[output.index, 'weight'] = output.weights
        return output_sizes.weight.fillna(0).to_dict()


def weight_group(data, logistic_spread, total_gross):

    def logistic_weight(k):
        return 2 / (1 + np.exp(-k)) - 1

    weights = np.apply_along_axis(
        logistic_weight, 0,
        np.linspace(-logistic_spread, logistic_spread, len(data)))
    weights = weights / np.abs(weights).sum() * total_gross

    data = data.sort_values('score')
    data.loc[:, 'weights'] = weights

    return data
