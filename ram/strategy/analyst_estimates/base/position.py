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
        self.comm = comm
        self.shares = 0
        self.exposure = 0
        self.daily_pl = 0
        self.daily_turnover = 0
        self.return_peak = 0
        self.cumulative_return = 0
        self.open_position = True
        self.current_price = float(price)
        self.sector = np.nan
        self.weight = 0.
        self.hold_days = -1
        # Check if position should even be opened
        if np.isnan(price) | (price == 0):
            self.open_position = False

    def update_position_prices(self, price):
        if not self.open_position:
            return
        elif np.isnan(price) | (price == 0):
            self.close_position()
            return
        self.daily_pl += (price - self.current_price) * self.shares
        self.current_price = float(price)
        self.exposure = self.shares * self.current_price
        if self.exposure != 0:
            self.cumulative_return += self.daily_pl / np.abs(self.exposure)
        return

    def update_position_size(self, new_size, exec_price):
        if not self.open_position:
            return
        elif np.isnan(exec_price) | (exec_price == 0):
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

    def split_adjustment(self, split):
        """
        NOTE:
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        self.shares = self.shares * split
        self.current_price = self.current_price / split
        return

    def dividend_adjustment(self, dividend):
        # Add dividend
        self.daily_pl += dividend * self.shares
        return

    def close_position(self):
        self.daily_pl += -1 * abs(self.shares) * self.comm
        self.daily_turnover += abs(self.shares) * self.current_price
        self.shares = 0
        self.exposure = 0
        self.position_weight = 0.

    # ~~~~~~  Getters and Setters~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_sector(self, sector):
        self.sector = sector

    def set_weight(self, weight):
        self.weight = weight

    def get_daily_pl(self):
        return float(self.daily_pl)

    def get_daily_turnover(self):
        return float(self.daily_turnover)

    def reset_daily_turnover(self):
        self.daily_turnover = 0.

    def reset_daily_pl(self):
        self.daily_pl = 0
