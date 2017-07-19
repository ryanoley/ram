import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.constructor.portfolio import Portfolio

from ram.strategy.long_pead.constructor.constructor2 import PortfolioConstructor2


class PortfolioConstructor3(PortfolioConstructor2):

    def get_daily_pl(self, arg_index, logistic_spread):
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
            mcaps = self.market_cap_dict[date]

            portfolio.update_prices(closes, dividends, splits)

            if date == self.iter_dates.iloc[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self._get_position_sizes_with_mcaps(
                    scores, mcaps, logistic_spread, self.booksize)
                portfolio.update_position_sizes(sizes, closes)

            daily_pl = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            daily_df.loc[date, 'PL'] = daily_pl
            daily_df.loc[date, 'Turnover'] = daily_turnover
            daily_df.loc[date, 'Exposure'] = daily_exposure

        return daily_df

    def _get_position_sizes_with_mcaps(self, scores, mcaps,
                                       logistic_spread, booksize):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        scoresA = pd.DataFrame({'scores': scores, 'mcaps': mcaps}).dropna()
        scoresA = scoresA.sort_values('mcaps')
        scores1 = scoresA.iloc[:len(scoresA)/2].copy()
        scores2 = scoresA.iloc[len(scoresA)/2:].copy()

        scores1.sort_values('scores', inplace=True)
        scores2.sort_values('scores', inplace=True)

        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1

        scores1['weights'] = [
            logistic_weight(x) for x in np.linspace(
                -logistic_spread, logistic_spread, len(scores1))]

        scores2['weights'] = [
            logistic_weight(x) for x in np.linspace(
                -logistic_spread, logistic_spread, len(scores2))]

        weights = scores1.append(scores2).weights
        allocations = (weights / weights.abs().sum() * booksize).to_dict()
        output = {x: 0 for x in scores.keys()}
        output.update(allocations)
        return output
