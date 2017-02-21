import numpy as np
import pandas as pd

from ram.strategy.statarb.constructor.position import MultiLegPosition


class PairPortfolio(object):

    def __init__(self):
        self.pairs = {}
        self.stats_container = {
            # Position level statistics
            'holding_days': [],
            'rebalance_count': [],
            'perc_gains': []
        }

    def update_prices(self, closes, dividends, splits):
        """
        Should be first step in daily loop!

        Parameters
        ----------
        closes/dividends/splits : dict
        """
        for pair in self.pairs.keys():
            self.pairs[pair].update_position_prices(closes, dividends, splits)
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
        base_exposure = float(base_exposure)
        for pair, pos in self.pairs.iteritems():
            flag1 = abs(pos.gross_exposure / base_exposure - 1) > perc_dev
            flag2 = abs(pos.net_exposure / base_exposure) > perc_dev
            flag3 = pos.open_position
            if (flag1 and flag3) or (flag2 and flag3):
                pos.update_position_exposure(base_exposure)
        return

    def add_pair(self, pair, trade_prices, gross_bet_size, side):
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
        # Split securities
        side1, side2 = pair.split('~')
        legs1 = np.array(side1.split('_'))
        legs2 = np.array(side2.split('_'))
        legs = np.append(legs1, legs2)
        # Get prices
        prices = np.array([trade_prices[l] for l in legs])
        # Get sizes
        sizes1 = np.repeat(gross_bet_size / 2 / len(legs1),
                           len(legs1)) * side
        sizes2 = np.repeat(gross_bet_size / 2 / len(legs2),
                           len(legs2)) * side * -1
        sizes = np.append(sizes1, sizes2)
        self.pairs[pair] = MultiLegPosition(legs, prices, sizes)
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
        """
        NOTE: Here the daily_pls are reset for individual positions,
        closed pairs are finally removed from the portfolio,
        and final statistics are gathered from those removed pairs.
        """
        port_daily_pl = 0
        # Get PL and Clean out positions
        for pair in self.pairs.keys():
            port_daily_pl += self.pairs[pair].daily_pl
            self.pairs[pair].daily_pl = 0
            if not self.pairs[pair].open_position:
                rpair = self.pairs.pop(pair)
                # Record some stats
                self.stats_container['holding_days'].append(
                    rpair.stat_holding_days)
                self.stats_container['rebalance_count'].append(
                    rpair.stat_rebalance_count)
                self.stats_container['perc_gains'].append(
                    rpair.stat_perc_gain)

        return port_daily_pl

    def get_period_stats(self):
        # Clean-up and return container
        stats = self.stats_container
        out = {}
        out['total_trades'] = len(stats['holding_days'])

        out['avg_holding_days'] = sum(stats['holding_days']) / float(
            len(stats['holding_days']))
        out['max_holding_days'] = max(stats['holding_days']) 

        out['avg_rebalance_count'] = sum(stats['rebalance_count']) / float(
            len(stats['rebalance_count']))
        out['max_rebalance_count'] = max(stats['rebalance_count'])

        out['avg_perc_gain'] = sum(stats['perc_gains']) / float(
            len(stats['perc_gains']))
        out['max_perc_gain'] = max(stats['perc_gains'])
        out['min_perc_gain'] = min(stats['perc_gains'])

        # Flush stats
        self.stats_container = {
            # Position level statistics
            'holding_days': [],
            'rebalance_count': [],
            'perc_gains': []
        }

        return out
