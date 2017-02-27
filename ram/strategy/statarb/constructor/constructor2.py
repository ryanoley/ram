import re
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio
from ram.strategy.statarb.constructor.base import BaseConstructor


class PortfolioConstructor2(BaseConstructor):

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize
        self._portfolio = PairPortfolio()

    def get_feature_names(self):
        """
        The columns from the database that are required.
        """
        return ['AdjClose', 'RClose', 'RCashDividend',
                'GSECTOR', 'SplitFactor']

    def get_iterable_args(self):
        return {
            'n_pairs': [100, 200, 300],
            'max_pos_prop': [0.05, 0.1],
            'pos_perc_deviation': [0.07, 0.14],
        }

    def get_daily_pl(self, n_pairs, max_pos_prop, pos_perc_deviation):
        """
        Parameters
        ----------
        n_pairs : int
            Number of pairs in the portfolio at the end of each day
        max_pos_prop : float
            Maximum proportion a single Security can be long/short
            given the number of pairs. For example, if there are 100 pairs
            and the max prop is 0.05, the max number of longs/shorts for
            Stock X is 5.
        pos_perc_deviation : float
            The max absolute deviation from the initial position before
            a rebalancing of the pair happens.
        """
        max_pos_count = int(n_pairs * max_pos_prop)

        # New portfolio created for each
        self._portfolio = PairPortfolio()

        # Output object
        daily_df = pd.DataFrame(index=self.all_dates,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        for date in self.all_dates:

            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            scores = self.enter_scores[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(closes, dividends, splits)

            # 2. GET UPDATED PORTFOLIO NAMES AND CLOSE POSITIONS
            new_positions = self._get_and_close_positions(
                scores, n_pairs, max_pos_count)

            # 3. ADJUST POSITIONS
            #  When the exposures move drastically (say when the markets)
            #  go up or down, it affects the size of the new positions
            #  quite significantly
            self._adjust_open_positions(n_pairs, pos_perc_deviation)

            # 4. OPEN NEW PAIRS - Just not last day of period
            if date != self.all_dates[-1]:
                self._execute_open_signals(new_positions, closes)

            # Report PL and Exposure
            daily_df.loc[date, 'PL'] = self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self._portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        self._portfolio.close_pairs(all_pairs=True)
        daily_df.loc[date, 'PL'] += self._portfolio.get_portfolio_daily_pl()
        daily_df.loc[date, 'Exposure'] = 0

        # Shift because with executing on Close prices should consider
        # yesterday's EOD exposure
        daily_df['Ret'] = daily_df.PL / daily_df.Exposure.shift(1)
        daily_df.Ret.iloc[0] = daily_df.PL.iloc[0] / \
            daily_df.Exposure.iloc[0]

        return daily_df.loc[:, ['Ret']], self._portfolio.get_period_stats()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_and_close_positions(self, scores, n_pairs, max_pos_count):
        new_positions = self._get_top_x_positions(scores,
                                                  n_pairs,
                                                  max_pos_count)
        for pair in self._portfolio.pairs.keys():
            if pair not in [x[1] for x in new_positions]:
                self._portfolio.pairs[pair].close_position()
        return new_positions

    def _adjust_open_positions(self, n_pairs, pos_perc_deviation=0.03):
        base_exposure = self.booksize / n_pairs
        self._portfolio.update_position_exposures(base_exposure,
                                                  pos_perc_deviation)

    def _execute_open_signals(self, positions, trade_prices):
        """
        Function that adds new positions.
        """
        open_pairs = self._portfolio.get_open_positions()
        gross_bet_size = self.booksize / len(positions)
        for sc, pair, side in positions:
            if pair in open_pairs:
                continue
            self._portfolio.add_pair(pair, trade_prices,
                                     gross_bet_size, side)
        return

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_top_x_positions(self, scores, n_pairs, max_pos_count):
        port_counts = {}
        new_positions = []
        for pos in scores:
            counts = self._split_position_and_count(pos)
            # See if any positions exceed exposure numbers
            if self._can_add(counts, port_counts, max_pos_count):
                port_counts = self._add_to_port(counts, port_counts)
                new_positions.append(pos)
            if len(new_positions) == n_pairs:
                break
        return new_positions

    @staticmethod
    def _can_add(counts, port_counts, max_pos_count):
        for key, val in counts.iteritems():
            if key in port_counts:
                if abs(port_counts[key] + val) > max_pos_count:
                    return False
        return True

    @staticmethod
    def _add_to_port(counts, port_counts):
        for key, val in counts.iteritems():
            if key in port_counts:
                port_counts[key] += val
            else:
                port_counts[key] = val
        return port_counts

    @staticmethod
    def _split_position_and_count(entry):
        pair = entry[1]
        side = entry[2]
        side1, side2 = re.split('[\~]', pair)
        legs1 = re.split('[\_]', side1)
        legs2 = re.split('[\_]', side2)
        # Safety measure
        assert len(set(re.split('[\_\~]', pair))) == len(legs1) + len(legs2), \
            'Duplicate SecCodes in position'
        out = {leg: side for leg in legs1}
        out.update({leg: -side for leg in legs2})
        return out
