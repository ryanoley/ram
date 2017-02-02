import numpy as np


class PairPosition(object):

    def __init__(self, leg1, p1, dollar_size1, leg2, p2, dollar_size2):
        """
        Parameters
        ----------
        leg1/leg2 : str
            The column header from the data files that is the ID
        p1/p2 : float
            The transaction prices (nominal)
        dollar_size1/dollar_size2 : numeric
            The dollar value of the position being put on. SIGN IS IMPORTANT.
        """
        # IDs, position name
        self.pair = '{0}_{1}'.format(leg1, leg2)
        self.leg1 = leg1
        self.leg2 = leg2
        # Never open position if no data
        if ~np.isnan(p1) & ~np.isnan(p2):
            # Number of shares
            self.shares1 = int(dollar_size1 / p1)
            self.shares2 = int(dollar_size2 / p2)
        else:
            self.shares1 = 0
            self.shares2 = 0
        # Current prices
        self.p1 = p1
        self.p2 = p2
        self.gross_exposure = abs(self.shares1) * p1 + abs(self.shares2) * p2
        self.daily_pl = 0

    def update_position_prices(self, p1, p2):
        if np.isnan(p1) | np.isnan(p2):
            # Assume position was closed
            self.daily_pl = -1 * self.gross_exposure * .0003
            self.gross_exposure = 0
        else:
            self.daily_pl = (p1 - self.p1) * self.shares1 + \
                (p2 - self.p2) * self.shares2
            self.gross_exposure = abs(p1 * self.shares1) + \
                abs(p2 * self.shares2)
        self.p1 = p1
        self.p2 = p2

    def get_daily_pl(self):
        pl = self.daily_pl
        self.daily_pl = 0
        return pl

    def get_cost(self):
        # Transaction costs 3 bps
        return -1 * self.gross_exposure * .0003
