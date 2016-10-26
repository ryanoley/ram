import numpy as np


class Position(object):
    pass


class PairPosition(Position):

    def __init__(self, leg1, p1, size1, leg2, p2, size2, comm=0.003):
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
        # IDs, position name
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
        # Current prices
        self.p1 = p1
        self.p2 = p2
        # Invoke every time one trades. Assumes same cost both in and
        # out.
        self.comm = comm
        # PL
        self.daily_pl = 0
        self.gross_exposure = abs(self.shares1) * p1 + abs(self.shares2) * p2

    def update_position_prices(self, p1, p2, d1=0, d2=0, sp1=1, sp2=1):
        """
        * p:   price
        * d:   dividend
        * sp:  split factor
        """
        # If split happened, adjust prices and shares
        if sp1 != 1:
            self.shares1 *= sp1
            self.p1 /= sp1
        if sp2 != 1:
            self.shares2 *= sp2
            self.p2 /= sp2

        # Only calculate if both not nan
        if np.isnan(p1) | np.isnan(p2):
            self.daily_pl = 0
            self.gross_exposure = 0
        else:
            self.daily_pl = (p1 - self.p1) * self.shares1 + \
                (p2 - self.p2) * self.shares2
            # Dividend income
            self.daily_pl += abs(self.shares1) * d1 + abs(self.shares2) * d2
            self.gross_exposure = abs(p1 * self.shares1) + \
                abs(p2 * self.shares2)
        self.p1 = p1
        self.p2 = p2

    @property
    def cost(self):
        return (np.abs(self.shares1) + np.abs(self.shares2)) * self.comm
