import numpy as np
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config
from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb.objects.portfolio import Portfolio

LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3
BOOKSIZE = 10e6


class BasePortfolioConstructor(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self._pricing = {}
        self._portfolios = {}
        self.booksize = BOOKSIZE

    @abstractmethod
    def get_args(self):
        raise NotImplementedError('BasePortfolioConstructor.get_args')

    @abstractmethod
    def set_args(self):
        raise NotImplementedError('BasePortfolioConstructor.set_args')

    @abstractmethod
    def set_signal_data(self):
        raise NotImplementedError('BasePortfolioConstructor.set_signal_data')

    @abstractmethod
    def set_other_data(self):
        raise NotImplementedError('BasePortfolioConstructor.set_other_data')

    @abstractmethod
    def get_day_position_sizes(self, date, signals):
        """
        Must have this interface
        """
        raise NotImplementedError(
            'BasePortfolioConstructor.get_day_position_sizes')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_test_dates(self, test_dates):
        self._test_dates = np.sort(test_dates)

    def set_pricing_data(self, time_index, data):
        """
        Combines previous and current pricing data in case there are still
        positions that are on
        """
        if (time_index - 2) in self._pricing:
            del self._pricing[(time_index - 2)]
        self._pricing[time_index] = data.copy()
        # Combine
        self._pricing['closes'] = \
            self._combine_pricing(time_index, 'RClose')
        self._pricing['divs'] = \
            self._combine_pricing(time_index, 'RCashDividend', 0)
        self._pricing['splits'] = \
            self._combine_pricing(time_index, 'SplitMultiplier', 0)

    def _combine_pricing(self, time_index, val_name, fill_val=np.nan):
        if (time_index - 1) in self._pricing:
            old_vals = make_variable_dict(self._pricing[time_index-1],
                                          val_name, fill_val)
        else:
            old_vals = {}
        new_vals = make_variable_dict(self._pricing[time_index],
                                      val_name, fill_val)
        all_dates = list(set(new_vals.keys() + old_vals.keys()))
        output = {}
        for d in all_dates:
            if d in old_vals:
                output[d] = old_vals[d]
            else:
                output[d] = {}
            if d in new_vals:
                output[d].update(new_vals[d])
        return output

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_period_daily_pl(self, column_index):

        if column_index in self._portfolios:
            portfolio = self._portfolios[column_index]
        else:
            portfolio = Portfolio()
            self._portfolios[column_index] = portfolio

        # Output object
        daily_df = pd.DataFrame(index=self._test_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for i, date in enumerate(self._test_dates):

            portfolio.update_prices(
                self._pricing['closes'][date],
                self._pricing['divs'][date],
                self._pricing['splits'][date])

            sizes = self.get_day_position_sizes(date, column_index)

            # Scale
            allocs = {k: v * BOOKSIZE for k, v in sizes.iteritems()}

            portfolio.update_position_sizes(allocs,
                                            self._pricing['closes'][date])

            pl_long, pl_short = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            # Min/Max position sizes
            exposures = [x.exposure for x in portfolio.positions.values()]
            daily_df.loc[date, 'PL'] = (pl_long + pl_short) / BOOKSIZE
            daily_df.loc[date, 'LongPL'] = pl_long / BOOKSIZE
            daily_df.loc[date, 'ShortPL'] = pl_short / BOOKSIZE
            daily_df.loc[date, 'Turnover'] = daily_turnover / BOOKSIZE
            daily_df.loc[date, 'Exposure'] = daily_exposure
            daily_df.loc[date, 'OpenPositions'] = sum([
                1 if x.shares != 0 else 0
                for x in portfolio.positions.values()])
            daily_df.loc[date, 'MaxPosSize'] = max(exposures)
            daily_df.loc[date, 'MinPosSize'] = min(exposures)

        return daily_df
