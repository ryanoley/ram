import numpy as np
import pandas as pd

from ram.strategy.statarb.constructor.position import PairPosition


class PairPortfolio(object):

    def __init__(self):
        self.pairs = {}

    def update_prices(self, closes, dividends, splits):
        """
        Should be first step in daily loop!

        Parameters
        ----------
        closes/dividends/splits : dict
        """
        for pair in self.pairs.keys():
            # Extract prices from dictionaries
            leg1, leg2 = pair.split('_')
            c1, c2 = closes[leg1], closes[leg2]
            d1, d2 = dividends[leg1], dividends[leg2]
            sp1, sp2 = splits[leg1], splits[leg2]
            # Only update the Position state, don't extract anything here.
            # Other adjustments need to happen downstream.
            self.pairs[pair].update_position_prices(c1, c2, d1, d2, sp1, sp2)
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
        for pair, pos in self.pairs.iteritems():
            flag1 = abs(pos.gross_exposure / base_exposure - 1) > perc_dev
            flag2 = abs(pos.net_exposure / base_exposure) > perc_dev
            flag3 = pos.open_position
            if (flag1 and flag3) or (flag2 and flag3):
                pos.update_position_exposure(base_exposure)
        return

    def add_pair(self, pair, trade_prices, dollar_size, side):
        """
        Parameters
        ----------
        pair : str
        trade_prices : Dict
            Key values should correspond to legs
        dollar_size : numeric
            The total gross exposure that should be put on
        side : 1, -1
            Going long the pair means going LONG Leg1 and SHORT Leg2
        """
        assert pair not in self.pairs.keys()
        assert side in [1, -1]
        leg1, leg2 = pair.split('_')
        leg_size = max(dollar_size / 2., 0.)
        self.pairs[pair] = PairPosition(
            leg1, trade_prices[leg1], side * leg_size,
            leg2, trade_prices[leg2], -1 * side * leg_size)
        return

    def close_pairs(self, close_pairs=None, all_pairs=False):
        if all_pairs:
            for pos in self.pairs.itervalues():
                pos.close_position()
            return
        for pair in close_pairs:
            self.pairs[pair].close_position()
        # Check if additional pairs to close due to bad data
        for pos in self.pairs.itervalues():
            if pos.to_close_position:
                pos.close_position()
        return

    def get_open_positions(self):
        return [pair for pair, pos in self.pairs.iteritems() \
                if pos.open_position]

    def count_open_positions(self):
        return sum([pos.open_position for pos in self.pairs.itervalues()])

    def get_gross_exposure(self):
        return sum([pos.gross_exposure for pos in self.pairs.itervalues()])

    def get_portfolio_daily_pl(self):
        port_daily_pl = 0
        # Get PL and Clean out positions
        for pair in self.pairs.keys():
            port_daily_pl += self.pairs[pair].daily_pl
            self.pairs[pair].daily_pl = 0
            if not self.pairs[pair].open_position:
                self.pairs.pop(pair)
        return port_daily_pl
