import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.birds.constructor.portfolio import Portfolio
from ram.strategy.birds.constructor.base import BaseConstructor


class PortfolioConstructor(BaseConstructor):

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize

    def get_daily_pl(self):
        """
        Parameters
        ----------
        """
        # New portfolio created for each
        self._portfolio = Portfolio()

        for date in self.all_dates:

            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(closes, dividends, splits)

            # Report PL and Exposure
            daily_df.loc[date, 'PL'] = self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self._portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        self._portfolio.close_all_pairs()
        daily_df.loc[date, 'PL'] += self._portfolio.get_portfolio_daily_pl()
        daily_df.loc[date, 'Exposure'] = 0

        # Shift because with executing on Close prices should consider
        # yesterday's EOD exposure
        daily_df['Ret'] = daily_df.PL / daily_df.Exposure.shift(1)
        daily_df.Ret.iloc[0] = daily_df.PL.iloc[0] / \
            daily_df.Exposure.iloc[0]

        return daily_df.loc[:, ['Ret']]

