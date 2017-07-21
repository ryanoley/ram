import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.constructor.portfolio import Portfolio

from ram.strategy.long_pead.constructor.constructor2 import PortfolioConstructor2


class PortfolioConstructor4(PortfolioConstructor2):

    def get_iterable_args(self):
        return {
            'logistic_spread': [0.1, 0.5, 1],
            'group_flag': [True, False]
        }

    def get_daily_pl(self, arg_index, logistic_spread, group_flag):
        """
        Parameters
        ----------
        """
        portfolio = Portfolio()
        # Output object
        daily_df = pd.DataFrame(index=self.iter_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for date in self.iter_dates:

            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            scores = self.scores_dict[date]
            # Could this be just a simple "Group"
            groups = self.group_dict[date]
            if not group_flag:
                groups = {x: 10 for x in groups.keys()}

            portfolio.update_prices(closes, dividends, splits)

            if date == self.iter_dates.iloc[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self._get_position_sizes_with_groups(
                    scores, groups, logistic_spread, self.booksize)
                portfolio.update_position_sizes(sizes, closes)

            daily_pl = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            daily_df.loc[date, 'PL'] = daily_pl
            daily_df.loc[date, 'Turnover'] = daily_turnover
            daily_df.loc[date, 'Exposure'] = daily_exposure

        return daily_df

    def _get_position_sizes_with_groups(self, scores, groups,
                                        logistic_spread, booksize):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        output = {x: 0 for x in scores.keys()}
        scores = pd.DataFrame({'scores': scores, 'groups': groups}).dropna()
        scores = scores.reset_index()
        # Subtract the mean from the groups
        groups = pd.DataFrame(scores.groupby('groups')['scores'].mean())
        groups.columns = ['meanScore']
        groups = groups.reset_index()

        scores = scores.merge(groups)
        scores['scores2'] = scores.scores - scores.meanScore

        scores.sort_values('scores2', inplace=True)

        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1

        scores['weights'] = [
            logistic_weight(x) for x in np.linspace(
                -logistic_spread, logistic_spread, len(scores))]

        weights = scores.weights
        weights.index = scores.loc[:, 'index']

        allocations = (weights / weights.abs().sum() * booksize).to_dict()

        output.update(allocations)
        return output
