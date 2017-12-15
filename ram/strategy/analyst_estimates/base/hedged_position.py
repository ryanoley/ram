import numpy as np

from ram.strategy.analyst_estimates.base.position import Position

class HedgedPosition(Position):

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
        super(HedgedPosition, self).__init__(symbol, price, comm)
        self.market_entry_price = 0.
        self.market_curent_price = 0.
        self.market_return = 0.

    def update_hedge_price(self, market_price):
        if 'HEDGE' not in market_price.keys():
            raise ValueError('HEDGE must be in key value in arg')
        mkt_px = market_price['HEDGE']

        # No position or just closed
        if self.exposure == 0.:
            self.market_entry_price = 0.
            self.market_curent_price = 0.
            return
        # Position just initiated
        elif self.market_entry_price == 0.:
            self.market_entry_price = mkt_px
            self.market_curent_price = mkt_px
            return

        self.market_curent_price = mkt_px
        self.market_return = (self.market_curent_price /
                                self.market_entry_price) - 1
        hedge_ret = self.market_return * np.sign(self.exposure)
        self.cumulative_return -= hedge_ret
        self.return_peak = np.max([self.cumulative_return, self.return_peak])

