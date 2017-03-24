import numpy as np


class Position(object):

    def __init__(self, symbol, entry_price, size, comm=0.005):
        """
        Parameters
        ----------
        symbol : str
            SecCode or Ticker
        entry_price : float
            The transaction prices (nominal)
        size : float
            The dollar value of the position being put on. SIGN IS IMPORTANT.
        comm : float
            Cost on both sides of transaction. Comm is the number of dollars
            per share rate
        """
        self.symbol = symbol
        # Flags to aid accounting
        self.open_position = True
        # Commission per share
        self.COMM = comm
        # Never open position if no data
        if ~np.isnan(entry_price):
            # Number of shares
            self.shares = int(size / entry_price)
        else:
            self.shares = 0
            self.open_position = False
        # Entry price
        self.price_entry = entry_price
        # Current prices
        self.price_current = entry_price
        # Cost of entering the position calculated here
        self.daily_pl = -1 * np.abs(self.shares) * comm
        # Exposure numbers
        self.exposure = self.shares * entry_price

    def update_position_price(self, prices, dividends=None, splits=None):
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
        new_price = prices[self.symbol]
        new_dividend = dividends[self.symbol] if dividends else 0
        new_split = splits[self.symbol] if splits else 1
        # Handle Splits
        if new_split != 1:
            self.shares = self.shares * new_split
            self.price_entry = self.price_entry / new_split
            self.price_current = self.price_current / new_split
        # Handle NAN prices
        if np.isnan(new_price):
            self.daily_pl = -1 * abs(self.shares) * self.COMM
            self.close_position()
        else:
            self.daily_pl = (new_price - self.price_current) * self.shares
            # Dividend income
            self.daily_pl += self.shares * new_dividend
            self.exposure = new_price * self.shares
        self.price_current = new_price
        return

    def update_position_exposure(self, new_exposure):
        """
        Will buy or sell shares to get back to this exposure
        """
        if self.open_position:
            new_shares = int(
                new_exposure / self.price_current) * np.sign(self.shares)
            trans_cost = abs(self.shares - new_shares) * self.COMM
            self.daily_pl -= trans_cost
            self.shares = new_shares
            self.exposure = self.price_current * self.shares

    def close_position(self):
        if self.open_position:
            self.daily_pl += -1 * abs(self.shares) * self.COMM
            self.shares = 0
            self.exposure = 0
            self.open_position = False
        return
