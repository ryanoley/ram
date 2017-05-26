import numpy as np


class Position(object):

    def __init__(self, symbol, price, size, comm=0.005):
        """
        Parameters
        ----------
        comm : float
            Cost on both sides of transaction. Comm is the number of dollars
            per share rate
        """
        self.symbol = symbol
        self.entry_price = float(price)
        self.current_price = float(price)
        self.size = float(size)
        self.comm = comm
        # Never open position if no pricing data
        self.open_position = True
        if ~np.isnan(price):
            self.shares = int(self.size / self.entry_price)
        else:
            self.shares = 0
            self.open_position = False
        self.exposure = self.shares * self.current_price
        # Cost of entering the position calculated here
        self.daily_pl = -1 * abs(self.shares) * self.comm
        self.total_pl = float(self.daily_pl)

    def update_position_prices(self, price, dividend, split):
        """
        NOTE:
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        if np.isnan(price):
            self.close_position()
            return
        self.daily_pl = 0
        # Handle splits
        if split != 1:
            self.shares = self.shares * split
            self.current_price = self.current_price / split
        self.daily_pl += (price - self.current_price) * self.shares
        if dividend:
            self.daily_pl += dividend * self.shares
        self.current_price = price
        self.exposure = self.shares * self.current_price
        self.total_pl += self.daily_pl
        return

    def update_position_size(self, new_size):
        if self.open_position:
            new_shares = int(new_size / self.current_price)
            d_shares = new_shares - self.shares
            self.shares = new_shares
            self.exposure = self.shares * self.current_price
            self.daily_pl += -1 * abs(d_shares) * self.comm

    def close_position(self):
        if self.open_position:
            self.daily_pl += -1 * abs(self.shares) * self.comm
            self.shares = 0
            self.open_position = False
        return
