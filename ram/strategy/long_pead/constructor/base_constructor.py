import numpy as np
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config
from ram.strategy.long_pead.utils import make_variable_dict
from ram.strategy.long_pead.constructor.portfolio import Portfolio


LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3


class Constructor(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_position_sizes(self, scores, **kwargs):
        raise NotImplementedError('Constructor.get_position_sizes')

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('Constructor.get_args')

    def __init__(self, booksize=10e6):
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
        portfolio = Portfolio()

        # Process needed values into dictionaries for efficiency
        scores = make_variable_dict(
            signals.preds_data, 'preds')
        closes = make_variable_dict(
            data_container.test_data, 'RClose')
        dividends = make_variable_dict(
            data_container.test_data, 'RCashDividend', 0)
        splits = make_variable_dict(
            data_container.test_data, 'SplitMultiplier', 1)
        liquidity = make_variable_dict(
            data_container.test_data, 'AvgDolVol')
        market_caps = make_variable_dict(
            data_container.test_data, 'MarketCap')
        sectors = make_variable_dict(
            data_container.test_data, 'GSECTOR')

        self.data_container = data_container
        self.signals = signals
        self.portfolio = portfolio

        # Dates to iterate over
        unique_test_dates = np.unique(data_container.test_data.Date)

        # Output object
        daily_df = pd.DataFrame(index=unique_test_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for i, date in enumerate(unique_test_dates):

            # If a low liquidity value, set score to nan
            # Update every five days
            if i % 5 == 0:
                low_liquidity_seccodes = filter_seccodes(
                    liquidity[date], LOW_LIQUIDITY_FILTER)
            # If close is very low, drop as well
            low_price_seccodes = filter_seccodes(
                closes[date], LOW_PRICE_FILTER)

            for seccode in set(low_liquidity_seccodes+low_price_seccodes):
                scores[date][seccode] = np.nan

            portfolio.update_prices(
                closes[date], dividends[date], splits[date])

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self.get_position_sizes(scores[date], **kwargs)
                sizes = self._scale_position_sizes_dollars(sizes)
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
            daily_df.loc[date, 'TicketChargePrc'] = \
                daily_stats['min_ticket_charge_prc']
            daily_df.loc[date, 'MeanSignal'] = np.nanmean(scores.values())
        # Time Index aggregate stats
        stats = {}
        return daily_df, stats

    def _scale_position_sizes_dollars(self, sizes):
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
