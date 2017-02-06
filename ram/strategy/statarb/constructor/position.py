import numpy as np


class PairPosition(object):

    # Commission
    COMM = 0.003

    def __init__(self, leg1, p1, size1, leg2, p2, size2):
        """
        Parameters
        ----------
        leg1/leg2 : str
            The column header from the data files that is the ID
        p1/p2 : float
            The transaction prices (nominal)
        size1/size2 : numeric
            The dollar value of the position being put on. SIGN IS IMPORTANT.
        comm : float
            Cost on both sides of transaction. Comm is the number of dollars
            per share rate
        """
        # Flags to aid accounting
        self.open_position = True
        self.to_close_position = False
        # IDs, position name
        self.pair = '{0}_{1}'.format(leg1, leg2)
        self.leg1 = leg1
        self.leg2 = leg2
        # Never open position if no data
        if ~np.isnan(p1) & ~np.isnan(p2):
            # Number of shares
            self.shares1 = int(size1 / p1)
            self.shares2 = int(size2 / p2)
        else:
            self.shares1 = 0
            self.shares2 = 0
            self.to_close_position = True
        # Entry prices
        self.p1_entry = p1
        self.p2_entry = p2
        # Current prices
        self.p1 = p1
        self.p2 = p2
        # Cost of entering the position calculated here
        self.daily_pl = -1 * (abs(self.shares1) +
                              abs(self.shares2)) * self.COMM
        self.gross_exposure = abs(self.shares1) * p1 + abs(self.shares2) * p2

    def update_position_prices(self, p1, p2, d1=0, d2=0, sp1=1, sp2=1):
        """
        * p:   price
        * d:   dividend
        * sp:  split multiplier

        NOTES
        -----
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        # Handle Splits
        if sp1 != 1:
            self.shares1 *= sp1
            self.p1_entry /= sp1
            self.p1 /= sp1
        if sp2 != 1:
            self.shares2 *= sp2
            self.p2_entry /= sp2
            self.p2 /= sp2

        # Handle NAN prices
        # Close position via setting gross position to zero.
        if np.isnan(p1) and ~np.isnan(p2):
            self.daily_pl = (p2 - self.p2) * self.shares2
            self.shares1 = 0
            self.to_close_position = True

        elif ~np.isnan(p1) and np.isnan(p2):
            self.daily_pl = (p1 - self.p1) * self.shares1
            self.shares2 = 0
            self.to_close_position = True

        elif np.isnan(p1) and np.isnan(p2):
            self.shares1 = 0
            self.shares2 = 0
            self.daily_pl = 0
            self.to_close_position = True

        # Calculate daily PL and update gross exposure
        else:
            self.daily_pl = (p1 - self.p1) * self.shares1 + \
                (p2 - self.p2) * self.shares2
            # Dividend income
            self.daily_pl += self.shares1 * d1 + self.shares2 * d2
            self.gross_exposure = abs(p1 * self.shares1) + \
                abs(p2 * self.shares2)

        self.p1 = p1
        self.p2 = p2

    def close_position(self):
        if self.open_position:
            self.daily_pl += -1 * (abs(self.shares1) +
                                   abs(self.shares2)) * self.COMM
            self.shares1 = 0
            self.shares2 = 0
            self.gross_exposure = 0
            self.open_position = False
        return
