import numpy as np
import pandas as pd
from ram.strategy.statarb.position import PairPosition


class Portfolio(object):
    pass


class PairPortfolio(Portfolio):

    def __init__(self):
        self.positions = {}
        self.daily_pl = 0

    def map_close_id_index(self, close):
        """
        Creates hash table for fast updating. Should be called before the
        daily loop in portfolio constructor so to speed up pair pricing
        access.
        """
        hashtable = dict(enumerate(close.columns))
        # Flip values
        self.id_hash = {z: y for y, z in hashtable.items()}

    def update_prices(self, close, dividends, splits):
        """
        At beginning of loop, update all the prices in the portfolio.
        This calculates the Daily PL for each position as well.
        """
        pairs = self.positions.keys()

        close1, close2, div1, div2, split1, split2 = self._get_close_prices(
            pairs, close, dividends, splits)

        for pair, c1, c2, d1, d2, sp1, sp2 in zip(
                pairs, close1, close2, div1, div2, split1, split2):
            self.positions[pair].update_position_prices(c1, c2, d1,
                                                        d2, sp1, sp2)
        # Get daily PL for all current positions
        self.daily_pl = self._positions_pl()
        return

    def add_pairs(self, pairs, sides, close, total_bet_size=1e5):
        """
        Optimized for speed.

        Parameters
        ----------
        pair : list-like
            Indicates pair name
        sides : list-like
            * 1 indicates go long first ticker
            * -1 indicates go short first ticker
        close : pd.Series
            Close prices assumed to be the transaction price
        total_bet_size : int
            Gross position to add
        """
        bet_size = total_bet_size / len(pairs) / 2
        # Get legs, and prices in individual arrays
        legs1, legs2 = self._split_pairs(pairs)
        close1, close2 = self._get_close_prices(pairs, close)

        for l1, l2, c1, c2, sd, p in zip(legs1, legs2,
                                         close1, close2,
                                         sides, pairs):
            if sd == 1:
                pos = PairPosition(l1, c1, bet_size,
                                   l2, c2, -bet_size)
            else:
                pos = PairPosition(l1, c1, -bet_size,
                                   l2, c2, bet_size)
            # Add position to portfolio
            self.positions[p] = pos
            # Subtract entry transaction costs from today's PL
            self.daily_pl -= pos.cost
        return

    def remove_pairs(self, pairs=None, all_pairs=False):
        if all_pairs:
            cost = sum([x.cost for x in self.positions.itervalues()])
            self.positions = {}
            del self.id_hash
            return cost
        # Remove pairs and get cost of making transaction.
        for pair in pairs:
            p = self.positions.pop(pair)
            self.daily_pl -= p.cost
        return

    def get_portfolio_daily_pl(self):
        """
        Returns portfolio daily pl and resets to zero. This function
        should be called at the end of the daily loop in a portfolio
        constructor.
        """
        dpl = self.daily_pl.copy()
        self.daily_pl = 0
        return dpl

    def get_gross_exposure(self):
        gross_exposure = 0
        for pos in self.positions.itervalues():
            gross_exposure += pos.gross_exposure
        return gross_exposure

    ###########################################################################
    # Special aggregator functionality for portfolio construction.

    def get_symbol_values(self):
        """
        Gets the total value for each ticker in the portfolio.
        """
        out = {}
        for pair in self.positions.itervalues():
            if pair.leg1 not in out:
                out[pair.leg1] = 0
            out[pair.leg1] += pair.p1 * pair.shares1
            if pair.leg2 not in out:
                out[pair.leg2] = 0
            out[pair.leg2] += pair.p2 * pair.shares2
        return out

    ###########################################################################
    # Private functions
    def _positions_pl(self):
        daily_pl = 0
        for pos in self.positions.itervalues():
            daily_pl += pos.daily_pl
        return daily_pl

    def _split_pairs(self, pairs):
        hold1 = []
        hold2 = []
        for pair in pairs:
            leg1, leg2 = pair.split('_')
            hold1.append(leg1)
            hold2.append(leg2)
        return hold1, hold2

    def _get_close_prices(self, pairs, close, dividends=None, splits=None):
        """
        Takes in current date's close prices, and returns two arrays
        that hold the prices for the pairs in the portfolio. ORDERED for
        efficiency.
        """
        close = np.array(close)
        dividends = np.array(dividends)
        splits = np.array(splits)

        legs1, legs2 = self._split_pairs(pairs)
        close1 = []
        for leg in legs1:
            close1.append(self.id_hash[leg])
        close2 = []
        for leg in legs2:
            close2.append(self.id_hash[leg])

        if len(dividends.shape):
            return close[close1], close[close2], \
                dividends[close1], dividends[close2], \
                splits[close1], splits[close2]
        else:
            return close[close1], close[close2]
