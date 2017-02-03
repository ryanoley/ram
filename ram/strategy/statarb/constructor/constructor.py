import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio
from ram.strategy.statarb.constructor.position import PairPosition
from ram.strategy.statarb.constructor.base import BaseConstructor


class PortfolioConstructor(BaseConstructor):

    def __init__(self,
                 n_pairs=[100],
                 max_pos_prop=[.05]
                 ):
        self._portfolio = PairPortfolio()
        self.booksize = 10e6
        # Params
        self.n_pairs = n_pairs
        # Maximum number of net positions on one side of the portfolio
        self.max_pos_prop = max_pos_prop

    def get_meta_params(self):
        return {'n_pairs': self.n_pairs,
                'max_pos_prop': self.max_pos_prop}

    def get_feature_names(self):
        return ['AdjClose', 'GSECTOR']

    def get_daily_pl(self, scores, data, pair_info, n_pairs, max_pos_prop):

        Close = data.pivot(index='Date',
                           columns='SecCode',
                           values='AdjClose')

        # Output object
        daily_df = pd.DataFrame(index=scores.index,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        for date in scores.index:

            # Get current period data
            cl_prices = Close.loc[date].to_dict()
            sc = scores.loc[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(cl_prices)

            # 2. CLOSE PAIRS IF NEEDED
            #  Closed pairs are still in portfolio dictionary
            #  and must be cleaned at end
            self._close_signals(sc, z_exit=1)

            # 3. OPEN PAIRS IF NEEDED
            # Must consider portfolio
            self._execute_open_signals(sc, cl_prices, n_pairs, max_pos_prop)

            # Report PL and Exposureexposure
            daily_df.loc[date, 'PL'] = self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self._portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        cost = self._portfolio.remove_pairs(all_pairs=True)
        daily_df.loc[date, 'PL'] += cost
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
        scores2 = scores.to_dict()
        # Remove positions that have gross exposure of zero from update_prices
        close_pairs = [nm for nm, pos in self._portfolio.pairs.iteritems()
                       if pos.gross_exposure == 0]

        # Get current position z-scores, and decide if they need to be closed
        for pair in self._portfolio.pairs.keys():
            if np.abs(scores2[pair]) < z_exit | np.isnan(scores2[pair]):
                close_pairs.append(pair)

        # Close positions
        self._portfolio.remove_pairs(list(set(close_pairs)))
        return

    def _execute_open_signals(self, scores, trade_prices,
                              n_pairs, max_pos_prop):
        """
        Function that adds new positions.
        """
        assert max_pos_prop > 0 and max_pos_prop < 1
        max_pos_count = int(n_pairs * max_pos_prop)
        current_pos = self._portfolio.pairs.keys()
        # Count new pairs, if none, then exit
        new_pairs = max(n_pairs - len(current_pos), 0)
        if new_pairs == 0:
            return

        scores2 = pd.DataFrame({'score': scores.abs(),
                                'side': scores > 0})
        scores2 = scores2.sort_values(['score'],
            ascending=False)

        # SELECTION
        # Current position values of all open values
        symbol_counts = self._portfolio.get_symbol_counts()

        bet_size = self.booksize - self._portfolio.get_gross_exposure()
        leg_size = bet_size / new_pairs / 2

        pairs = []
        sides = []

        for pair, (sc, side) in scores2.iterrows():

            leg1, leg2 = pair.split('_')

            # Check if hit max value long for position
            if leg1 in symbol_counts:
                if abs(symbol_counts[leg1]) == max_pos_count:
                    continue
                symbol_counts[leg1] += -1 if side else 1
            else:
                symbol_counts[leg1] = -1 if side else 1

            if leg2 in symbol_counts:
                if abs(symbol_counts[leg2]) == max_pos_count:
                    continue
                symbol_counts[leg2] += 1 if side else -1
            else:
                symbol_counts[leg2] = 1 if side else -1

            # Create position
            if side:
                pos = PairPosition(leg1, trade_prices[leg1], -leg_size,
                                   leg2, trade_prices[leg2], leg_size)
            else:
                pos = PairPosition(leg1, trade_prices[leg1], leg_size,
                                   leg2, trade_prices[leg2], -leg_size)
            self._portfolio.add_pair(pos)

            if len(self._portfolio.pairs) == n_pairs:
                break

        return
