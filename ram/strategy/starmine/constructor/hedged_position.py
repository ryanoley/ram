import numpy as np

from ram.strategy.starmine.constructor.position import Position

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
        self.market_entry_price = 0
        self.market_curent_price = 0
        self.market_return = 0
        self.sector = np.nan
        self.weight = 0.
        self.hold_days = -1

    def update_position_prices(self, price, dividend, split):
        """
        NOTE:
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        if not self.open_position:
            return
        elif np.isnan(price) | (price == 0):
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
        if self.exposure != 0:
            self.cumulative_return += self.daily_pl / np.abs(self.exposure)
        return

    def update_mkt_prices(self, market_price):
        if 'HEDGE' not in market_price.keys():
            raise ValueError('HEDGE must be in key value in arg')
        mkt_px = market_price['HEDGE']

        # No position or just closed
        if self.exposure == 0:
            self.market_entry_price = 0
            self.market_curent_price = 0
            return
        # Position just initiated
        elif self.market_entry_price == 0:
            self.market_entry_price = mkt_px
            self.market_curent_price = mkt_px
            return

        self.market_curent_price = mkt_px
        self.market_return = (self.market_curent_price /
                                self.market_entry_price) - 1
        hedge_ret = self.market_return * np.sign(self.exposure)
        self.cumulative_return -= hedge_ret
        self.return_peak = np.max([self.cumulative_return, self.return_peak])

    def set_sector(self, sector):
        self.sector = sector

    def set_weight(self, weight):
        self.weight = weight

    def close_position(self):
        self.daily_pl += -1 * abs(self.shares) * self.comm
        self.daily_turnover = abs(self.shares) * self.current_price
        self.shares = 0
        self.exposure = 0
        self.position_weight = 0.
