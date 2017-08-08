import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio

LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3


class PortfolioConstructor1(object):

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize

    def get_args(self):
        return {
            'logistic_spread': [0.01, 0.1, 0.5, 1]
        }

    def get_daily_pl(self, data_container, signals, logistic_spread):
        """
        Parameters
        ----------
        """
        close_dict = make_variable_dict(
            data_container.test_data, 'RClose')
        dividend_dict = make_variable_dict(
            data_container.test_data, 'RCashDividend', 0)
        split_mult_dict = make_variable_dict(
            data_container.test_data, 'SplitMultiplier', 1)
        scores_dict = make_variable_dict(data_container.test_data, 'preds')

        liquidity_dict = make_variable_dict(
            data_container.test_data, 'AvgDolVol')

        portfolio = Portfolio()

        unique_test_dates = np.unique(data_container.test_data.Date)

        # Output object
        daily_df = pd.DataFrame(index=unique_test_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for i, date in enumerate(unique_test_dates):

            closes = close_dict[date]
            dividends = dividend_dict[date]
            splits = split_mult_dict[date]
            scores = scores_dict[date]

            # If a low liquidity value, set score to nan
            # Update every five days
            if i % 5 == 0:
                low_liquidity_seccodes = filter_seccodes(
                    liquidity_dict[date], LOW_LIQUIDITY_FILTER)
            # If close is very low
            low_price_seccodes = filter_seccodes(closes, LOW_PRICE_FILTER)

            for seccode in set(low_liquidity_seccodes+low_price_seccodes):
                scores[seccode] = np.nan

            # Accounting
            portfolio.update_prices(closes, dividends, splits)

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self._get_position_sizes(scores, logistic_spread,
                                                 self.booksize)
                portfolio.update_position_sizes(sizes, closes)

            daily_pl = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            daily_df.loc[date, 'PL'] = daily_pl
            daily_df.loc[date, 'Turnover'] = daily_turnover
            daily_df.loc[date, 'Exposure'] = daily_exposure
            daily_df.loc[date, 'OpenPositions'] = sum([
                1 if x.shares != 0 else 0
                for x in portfolio.positions.values()])

        # Close everything and begin anew in new quarter
        return daily_df

    def _get_position_sizes(self, scores, logistic_spread, booksize):
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
        scores.weights = scores.weights / scores.weights.abs().sum() * booksize
        return scores.weights.to_dict()


def filter_seccodes(data_dict, min_value):
    bad_seccodes = []
    for key, value in data_dict.iteritems():
        if value < min_value:
            bad_seccodes.append(key)
    return bad_seccodes
