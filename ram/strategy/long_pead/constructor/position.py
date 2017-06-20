import numpy as np


class Position(object):

    def __init__(self, symbol, price, comm=0.005):
        """
        Parameters
        ----------
        symbol : str
            Symbol level identifier
        price : float
            This is important as it will be the value that new shares
            are calculated from.
        comm : float
            Commissions
        """
        self.symbol = symbol
        self.current_price = float(price)
        self.comm = comm
        self.open_position = True
        self.shares = 0
        self.exposure = 0
        self.daily_pl = 0
        self.daily_turnover = 0

    def update_position_prices(self, price, dividend, split):
        """
        NOTE:
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        if np.isnan(price) | (not self.open_position):
            self.close_position()
            return
        # Handle splits
        if split != 1:
            self.shares = self.shares * split
            self.current_price = self.current_price / split
        self.daily_pl += (price - self.current_price) * self.shares
        if dividend:
            self.daily_pl += dividend * self.shares
        self.current_price = float(price)
        self.exposure = self.shares * self.current_price
        return

    def update_position_size(self, new_size, exec_price):
        if np.isnan(exec_price) | (not self.open_position):
            self.close_position()
            return
        new_shares = int(new_size / self.current_price)
        d_shares = new_shares - self.shares
        # Update PL and set current price to whatever executed at
        # Important for difference between EOD Close and BOD Open executions
        self.daily_pl += (float(exec_price) - self.current_price) * self.shares
        self.current_price = float(exec_price)
        # Housekeeping
        self.shares = new_shares
        self.exposure = self.shares * self.current_price
        self.daily_pl += -1 * abs(d_shares) * self.comm
        self.daily_turnover += abs(d_shares) * float(exec_price)

    def close_position(self):
        self.daily_pl += -1 * abs(self.shares) * self.comm
        self.daily_turnover = abs(self.shares) * self.current_price
        self.shares = 0
        self.exposure = 0
        self.open_position = False

    # ~~~~~~  Getters  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_daily_pl(self):
        """
        This PL reset mechanism is very important for getting the accounting
        correct between simulations that execute at the EOD close or the
        next day open!!!
        """
        daily_pl = float(self.daily_pl)
        self.daily_pl = 0
        return daily_pl

    def get_daily_turnover(self):
        daily_turnover = float(self.daily_turnover)
        self.daily_turnover = 0
        return daily_turnover
