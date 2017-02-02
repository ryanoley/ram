import numpy as np
import pandas as pd

from ram.strategy.statarb.constructor.position import PairPosition


class PairPortfolio(object):

    def __init__(self):
        self.pairs = {}
        self.port_daily_pl = 0

    def update_prices(self, close):
        """
        At beginning of loop, update all the prices in the portfolio.
        This calculates the Daily PL for each position as well.
        """
        for pair in self.pairs.keys():
            leg1, leg2 = pair.split('_')
            self.pairs[pair].update_position_prices(close[leg1], close[leg2])
            self.port_daily_pl += self.pairs[pair].get_daily_pl()
        return

    def add_pair(self, new_pos):
        self.pairs[new_pos.pair] = new_pos
        # Subtract entry transaction costs from today's PL
        self.port_daily_pl += new_pos.get_cost()
        return

    def remove_pairs(self, remove_pairs=None, all_pairs=False):
        if all_pairs:
            cost = sum([x.get_cost() for x in self.pairs.itervalues()])
            self.pairs = {}
            return cost
        # Remove pairs and get cost of making transaction.
        for pair in remove_pairs:
            p = self.pairs.pop(pair)
            self.port_daily_pl += p.get_cost()
        return

    def get_portfolio_daily_pl(self):
        """
        Returns portfolio daily pl and resets to zero. This function
        should be called at the end of the daily loop in a portfolio
        constructor.
        """
        dpl = self.port_daily_pl.copy()
        self.port_daily_pl = 0
        return dpl

    def get_gross_exposure(self):
        gross_exposure = 0
        for pos in self.pairs.itervalues():
            gross_exposure += pos.gross_exposure
        return gross_exposure

    ###########################################################################
    # Special aggregator functionality for portfolio construction.

    def get_symbol_counts(self):
        """
        Gets the total net long positions
        """
        out = {}
        for pair in self.pairs.itervalues():
            if pair.leg1 not in out:
                out[pair.leg1] = 0
            out[pair.leg1] += 1 if pair.shares1 > 0 else -1

            if pair.leg2 not in out:
                out[pair.leg2] = 0
            out[pair.leg2] += 1 if pair.shares2 > 0 else -1

        return out
