import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio
from ram.strategy.statarb.constructor.position import PairPosition
from ram.strategy.statarb.constructor.base import BaseConstructor


class PortfolioConstructor(BaseConstructor):

    def __init__(self):
        self._portfolio = PairPortfolio()
        self.n_pairs = 100
        self.booksize = 10e6
        # Maximum number of net positions on one side of the portfolio
        self.max_pos_count = 5

    def get_daily_pl(self, scores, data, pair_info):

        Close = data.pivot(index='Date',
                           columns='SecCode',
                           values='AdjClose')

        # Output object
        daily_df = pd.DataFrame(index=scores.index,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        for date in scores.index:
            # Get current period data
            cl = Close.loc[date]
            sc = scores.loc[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(cl)

            # 2. CLOSE PAIRS IF NEEDED
            #  Closed pairs are still in portfolio dictionary
            #  and must be cleaned at end
            self._close_signals(sc, z_exit=1)

            # 3. OPEN PAIRS IF NEEDED
            # Must consider portfolio
            self._execute_open_signals(sc, cl)

            # Report PL and Exposureexposure
            daily_df.loc[date, 'PL'] = self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self._portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        cost = self._portfolio.remove_pairs(all_pairs=True)
        daily_df.loc[date, 'PL'] -= cost
        daily_df.loc[date, 'Exposure'] = 0
        daily_df['Ret'] = daily_df.PL / daily_df.Exposure
        # Compensate for closed exposure
        daily_df.Ret.iloc[-1] = daily_df.PL.iloc[-1] / \
            daily_df.Exposure.iloc[-2]
        return daily_df.loc[:, ['Ret']]

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _close_signals(self, scores, z_exit=1):
        """
        Innovation can happen here
        """
        # Remove positions that have gross exposure of zero from update_prices
        close_pairs = [nm for nm, pos in self._portfolio.pairs.iteritems()
                       if pos.gross_exposure == 0]

        # Get current position z-scores, and decide if they need to be closed
        for pair in self._portfolio.pairs.keys():
            if np.abs(scores[pair]) < z_exit | np.isnan(scores[pair]):
                close_pairs.append(pair)

        # Close positions
        self._portfolio.remove_pairs(close_pairs)
        return

    def _execute_open_signals(self, scores, trade_prices):
        """
        Function that adds new positions.
        """
        current_pos = self._portfolio.pairs.keys()
        # Count new pairs, if none, then exit
        new_pairs = self.n_pairs - len(current_pos)
        if new_pairs == 0:
            return
        scores = scores.dropna()
        # Remove those currently in positions
        scores = scores[~scores.index.isin(current_pos)]
        scores = scores[np.argsort(np.abs(scores.values))][::-1]

        # SELECTION
        # Current position values of all open values
        symbol_counts = self._portfolio.get_symbol_counts()

        bet_size = self.booksize - self._portfolio.get_gross_exposure()
        leg_size = bet_size / new_pairs / 2

        pairs = []
        sides = []

        for pair, sc in scores.iteritems():

            leg1, leg2 = pair.split('_')

            # Check if hit max value long for position
            if leg1 in symbol_counts:
                if abs(symbol_counts[leg1]) == self.max_pos_count:
                    continue
                symbol_counts[leg1] += 1 if sc < 0 else -1
            else:
                symbol_counts[leg1] = 1 if sc < 0 else -1

            if leg2 in symbol_counts:
                if abs(symbol_counts[leg2]) == self.max_pos_count:
                    continue
                symbol_counts[leg2] += 1 if sc > 0 else -1
            else:
                symbol_counts[leg2] = 1 if sc > 0 else -1

            # Create position
            if sc >= 0:
                pos = PairPosition(leg1, trade_prices[leg1], -leg_size,
                                   leg2, trade_prices[leg2], leg_size)
            else:
                pos = PairPosition(leg1, trade_prices[leg1], leg_size,
                                   leg2, trade_prices[leg2], -leg_size)
            self._portfolio.add_pair(pos)

            if len(self._portfolio.pairs) == self.n_pairs:
                break

        return
