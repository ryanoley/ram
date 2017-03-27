import numpy as np
import pandas as pd

from ram.strategy.birds.constructor.position import Position


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
        for symbol in self.positions.keys():
            self.positions[symbol].update_position_price(
                closes, dividends, splits)
        return

    def update_position_exposures(self, base_exposure, perc_dev):
        """
        Parameters
        ----------
        base_exposure : numeric
            Dollar value of the base exposure
        perc_dev : numeric
            Percent deviation from the base exposure that is allowed
            before the position is corrected.
        """
        for symbol, pos in self.positions.iteritems():
            flag1 = abs(pos.exposure) / base_exposure - 1 > perc_dev
            flag2 = pos.open_position
            if flag1 and flag2:
                pos.update_position_exposure(base_exposure)
        return

    def add_position(self, symbol, trade_prices, bet_size):
        """
        Parameters
        ----------
        pair : str
        trade_prices : Dict
            Key values should correspond to legs
        dollar_size : numeric
            The total gross exposure that should be put on. Negative
            means it will be a short position
        """
        assert symbol not in self.positions
        self.positions[symbol] = Position(symbol,
                                          trade_prices[symbol],
                                          bet_size)
        return

    def close_positions(self, close_positions):
        for symbol in close_positions:
            self.positions[symbol].close_position()
        return

    def close_all_positions(self):
        for pos in self.positions.itervalues():
            pos.close_position()
        return

    # ~~~~~~ EOD Accounting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_gross_exposure(self):
        return sum([abs(pos.exposure) for pos in self.positions.itervalues()])

    def get_portfolio_daily_pl(self):
        port_daily_pl = 0
        for symbol in self.positions.keys():
            port_daily_pl += self.positions[symbol].daily_pl
            if not self.positions[symbol].open_position:
                rpos = self.positions.pop(symbol)
        return port_daily_pl
