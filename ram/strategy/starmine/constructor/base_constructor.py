import numpy as np
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config

from ram.strategy.basic.constructor.portfolio import Portfolio
from ram.strategy.basic.utils import make_variable_dict


LOW_PRICE_FILTER = 7


class Constructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_position_sizes(self):
        raise NotImplementedError('Constructor.get_position_sizes')

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('Constructor.get_args')

    def __init__(self, booksize=30e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize

    def get_daily_pl(self, data_container, signals, **kwargs):
        """
        Parameters
        ----------
        data_container
        signals
        kwargs
        """
        scores_dict = make_variable_dict(signals.preds_data, 'preds')

        portfolio = Portfolio()

        unique_test_dates = np.unique(data_container.test_data.Date)
        daily_pl_data = data_container.daily_pl_data.copy()

        # Output object
        daily_df = pd.DataFrame(index=unique_test_dates, dtype=float)

        for i, date in enumerate(unique_test_dates):

            scores = scores_dict[date]

            closes = data_container.close_dict[date]
            dividends = data_container.dividend_dict[date]
            splits = data_container.split_mult_dict[date]
            liquidity = data_container.liquidity_dict[date]

            # If close is very low, drop as well
            low_price_seccodes = filter_seccodes(closes, LOW_PRICE_FILTER)

            for seccode in set(low_price_seccodes):
                scores[seccode] = np.nan

            portfolio.update_prices(closes, dividends, splits)

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self._get_position_sizes_dollars(
                    self.get_position_sizes(scores, daily_pl_data, **kwargs))
                portfolio.update_position_sizes(sizes, closes)

            pl_long, pl_short = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            daily_df.loc[date, 'PL'] = pl_long + pl_short
            daily_df.loc[date, 'LongPL'] = pl_long
            daily_df.loc[date, 'ShortPL'] = pl_short
            daily_df.loc[date, 'Turnover'] = daily_turnover
            daily_df.loc[date, 'Exposure'] = daily_exposure
            daily_df.loc[date, 'OpenPositions'] = sum([
                1 if x.shares != 0 else 0
                for x in portfolio.positions.values()])
            # Daily portfolio stats
            daily_stats = portfolio.get_portfolio_stats()
            daily_df.loc[date, 'stat1'] = daily_stats['stat1']
        # Time Index aggregate stats
        stats = {}
        return daily_df, stats

    def _get_position_sizes_dollars(self, sizes):
        """
        Setup to normalize outputs from derived class. Uses booksize
        to convert to dollars
        """
        if isinstance(sizes, dict):
            sizes = pd.Series(sizes)
        return (sizes / sizes.abs().sum() * self.booksize).to_dict()


def filter_seccodes(data_dict, min_value):
    bad_seccodes = []
    for key, value in data_dict.iteritems():
        if value < min_value:
            bad_seccodes.append(key)
    return bad_seccodes
