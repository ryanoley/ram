import numpy as np


class PairPosition(object):

    def __init__(self, leg1, leg2, price1, price2, size1, size2, comm=0.005):
        """
        Parameters
        ----------
        legs : numpy arrays of positions
            The column header from the data files that is the ID
        prices : numpy arrays of positions
            The transaction prices (nominal)
        sizes : numpy arrays of positions
            The dollar value of the position being put on. SIGN IS IMPORTANT.
        comm : float
            Cost on both sides of transaction. Comm is the number of dollars
            per share rate
        """
        self.leg1 = leg1
        self.leg2 = leg2
        # Flags to aid accounting
        self.open_position = True
        # Commission per share
        self.COMM = comm
        # Never open position if no data
        if ~np.isnan(price1) and ~np.isnan(price2):
            # Number of shares
            self.shares1 = int(size1 / price1)
            self.shares2 = int(size2 / price2)
        else:
            self.shares1 = 0
            self.shares2 = 0
            self.open_position = False
        # Current prices
        self.prices_current1 = price1
        self.prices_current2 = price2
        # Cost of entering the position calculated here
        self.daily_pl = -1 * (abs(self.shares1) + abs(self.shares2)) * comm
        self.total_pl = float(self.daily_pl)
        # Exposure numbers
        self._calculate_exposures()
        # Entry exposure
        self.entry_exposure = self.gross_exposure
        # Stats
        self.stat_holding_days = 0
        self.stat_rebalance_count = 0

    def update_position_prices(self, prices, dividends, splits):
        """
        Parameters
        ----------
        prices : dict
        dividends : dict
        splits : dict

        NOTES
        -----
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        self.daily_pl = 0
        p1 = prices[self.leg1]
        p2 = prices[self.leg2]
        # Handle Splits
        if splits[self.leg1] != 1:
            self.shares1 = self.shares1 * splits[self.leg1]
            self.prices_current1 = self.prices_current1 / splits[self.leg1]
        if splits[self.leg2] != 1:
            self.shares2 = self.shares2 * splits[self.leg2]
            self.prices_current2 = self.prices_current2 / splits[self.leg2]
        if np.isnan(p1):
            self.open_position = False
            self.daily_pl += -1 * abs(self.shares1) * self.COMM
        else:
            # Prices
            self.daily_pl += (p1 - self.prices_current1) * self.shares1
            if dividends[self.leg1]:
                self.daily_pl += dividends[self.leg1] * self.shares1
            self.prices_current1 = p1
        if np.isnan(p2):
            self.open_position = False
            self.daily_pl += -1 * abs(self.shares2) * self.COMM
        else:
            # Prices
            self.daily_pl += (p2 - self.prices_current2) * self.shares2
            if dividends[self.leg2]:
                self.daily_pl += dividends[self.leg2] * self.shares2
            self.prices_current2 = p2
        self._calculate_exposures()
        # Stats
        self.total_pl += self.daily_pl
        self.stat_holding_days += 1
        return

    def update_position_exposure(self, new_gross_exposure):
        """
        Returns net exposure to zero.
        """
        if self.open_position:
            side_exp = new_gross_exposure / 2.
            side_mult = 1 if self.shares1 > 0 else -1
            new_shares1 = int(side_exp / self.prices_current1) * side_mult
            new_shares2 = int(side_exp / self.prices_current2) * side_mult * -1
            trans_cost = (abs(self.shares1 - new_shares1) +
                          abs(self.shares2 - new_shares2)) * self.COMM
            self.daily_pl -= trans_cost
            self.shares1 = new_shares1
            self.shares2 = new_shares2
            self._calculate_exposures()
            self.stat_rebalance_count += 1

    def _calculate_exposures(self):
        self.net_exposure = \
            (self.shares1 * self.prices_current1) + \
            (self.shares2 * self.prices_current2)
        self.gross_exposure = \
            (abs(self.shares1) * self.prices_current1) + \
            (abs(self.shares2) * self.prices_current2)

    def close_position(self):
        if self.open_position:
            self.daily_pl -= (abs(self.shares1) +
                              abs(self.shares2)) * self.COMM
            self.shares1 = 0
            self.shares2 = 0
            self.net_exposure = 0
            self.gross_exposure = 0
            self.open_position = False
        return
