import numpy as np
import pandas as pd

from ram.strategy.long_pead.constructor.position import Position


class Portfolio(object):

    def __init__(self):
        self.positions = {}

    def update_prices(self, closes, dividends, splits):
        """
        Should be first step in daily loop!

        Parameters
        ----------
        closes/dividends/splits : dict
        """
        for symbol, close_ in closes.iteritems():
            if symbol not in self.positions:
                self.positions[symbol] = Position(
                    symbol=symbol, price=close_, size=0)
            else:
                self.positions[symbol].update_position_prices(
                    close_, dividends[symbol], splits[symbol])
        return

    def update_position_sizes(self, sizes):
        for symbol, size in sizes.iteritems():
            self.positions[symbol].update_position_size(size)
        return

    def get_exposure(self):
        return sum([abs(pos.exposure) for pos in self.positions.itervalues()])

    def get_portfolio_daily_pl(self):
        port_daily_pl = 0
        for position in self.positions.values():
            port_daily_pl += position.daily_pl
        return port_daily_pl