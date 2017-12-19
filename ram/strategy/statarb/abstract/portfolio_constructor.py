import numpy as np
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config
from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb.abstract.portfolio import Portfolio

LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3
BOOKSIZE = 10e6


class BasePortfolioConstructor(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.booksize = BOOKSIZE

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BasePortfolioConstructor.get_args')

    @abstractmethod
    def set_args(self):
        raise NotImplementedError('BasePortfolioConstructor.set_args')

    @abstractmethod
    def set_signals_constructor_data(self):
        raise NotImplementedError('BasePortfolioConstructor.'
                                  'set_signals_constructor_data')

    @abstractmethod
    def get_day_position_sizes(self, date, signals):
        """
        Must have this interface
        """
        raise NotImplementedError(
            'BasePortfolioConstructor.get_day_position_sizes')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_period_daily_pl(self):

        portfolio = Portfolio()
        self.portfolio = portfolio

        # Process needed values into dictionaries for efficiency
        scores = make_variable_dict(self._signals, 'preds')
        closes = make_variable_dict(self._pricing, 'RClose')
        dividends = make_variable_dict(self._pricing, 'RCashDividend', 0)
        splits = make_variable_dict(self._pricing, 'SplitMultiplier', 1)
        liquidity = make_variable_dict(self._pricing, 'AvgDolVol')
        market_cap = make_variable_dict(self._pricing, 'MarketCap')
        # Dates to iterate over
        unique_test_dates = np.unique(self._pricing.Date)

        # Output object
        daily_df = pd.DataFrame(index=unique_test_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for i, date in enumerate(unique_test_dates):

            # If a low liquidity/price value, set score to nan
            # Update every five days
            if i % 5 == 0:
                low_liquidity_seccodes = filter_seccodes(
                    liquidity[date], LOW_LIQUIDITY_FILTER)
                low_price_seccodes = filter_seccodes(
                    closes[date], LOW_PRICE_FILTER)

            for seccode in set(low_liquidity_seccodes+low_price_seccodes):
                scores[date][seccode] = np.nan

            portfolio.update_prices(
                closes[date], dividends[date], splits[date])

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self.get_day_position_sizes(date, scores[date])
                portfolio.update_position_sizes(sizes, closes[date])

            pl_long, pl_short = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            min_pos_size = min([pos.exposure for pos
                                in portfolio.positions.values()])
            max_pos_size = max([pos.exposure for pos
                                in portfolio.positions.values()])

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
            daily_df.loc[date, 'TicketChargePrc'] = \
                daily_stats['min_ticket_charge_prc']
            daily_df.loc[date, 'MeanSignal'] = \
                np.nanmean(scores[date].values())
            daily_df.loc[date, 'MinPosSize'] = min_pos_size / self.booksize
            daily_df.loc[date, 'MaxPosSize'] = max_pos_size / self.booksize
        # Time Index aggregate stats
        stats = {}
        return daily_df, stats


def filter_seccodes(data_dict, min_value):
    bad_seccodes = []
    for key, value in data_dict.iteritems():
        if value < min_value:
            bad_seccodes.append(key)
    return bad_seccodes
