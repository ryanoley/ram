import numpy as np


class Portfolio(object):

    def __init__(self, booksize=10e6):
        self.booksize = booksize

    def update_positions(self, alphas, prices):
        # New positions every-day
        self._positions = {}
        # Count longs/shorts
        longs = 0
        shorts = 0
        for x in alphas.values():
            if x > 0:
                longs += 1
            elif x < 0:
                shorts += 1
        # Long and short position sizes
        long_size = self.booksize / 2 / float(longs)
        short_size = self.booksize / 2 / float(shorts)
        for symbol, alpha in alphas.iteritems():
            if alpha > 0:
                self._positions[symbol] = long_size
            elif alpha < 0:
                self._positions[symbol] = -short_size

    def update_prices(self, prices):
        if not hasattr(self, '_positions'):
            # Init portfolio
            self._positions = {symbol: 0 for symbol in prices.keys()}
            self._prices = prices
            return 0
        else:
            daily_pl = 0
            for sym, pos in self._positions.iteritems():
                if pos != 0:
                    if np.isnan(prices[sym]) | np.isnan(self._prices[sym]):
                        continue
                    daily_pl += (prices[sym]/float(self._prices[sym]) - 1) * \
                        self._positions[sym]
            self._prices = prices
            return daily_pl / self.booksize
