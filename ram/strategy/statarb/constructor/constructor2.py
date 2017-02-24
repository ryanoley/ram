import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class PortfolioConstructor2(PortfolioConstructor):

    def get_iterable_args(self):
        return {
            'n_pairs': [50, 100, 200],
            'max_pos_prop': [0.05, 0.1],
            'pos_perc_deviation': [0.07, 0.14]
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

            exit_scores = self.exit_scores[date]
            enter_scores = self.enter_scores[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(closes, dividends, splits)

            # 2. CLOSE PAIRS
            #  Closed pairs are still in portfolio dictionary
            #  and must be cleaned at end
            self._close_signals(enter_scores, n_pairs)

            # 3. ADJUST POSITIONS
            #  When the exposures move drastically (say when the markets)
            #  go up or down, it affects the size of the new positions
            #  quite significantly
            self._adjust_open_positions(n_pairs, pos_perc_deviation)

            # 4. OPEN NEW PAIRS - Just not last day of periodn
            if date != self.all_dates[-1]:
                self._execute_open_signals(enter_scores, closes,
                                           n_pairs, max_pos_prop, z_exit=0.5)

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

    def _close_signals(self, scores, n_pairs):

        close_pairs = []

        # ~~ ADD LOGIC HERE FOR CLOSING WILD MOVERS ~~ #
        top_pairs = [x[1] for x in scores[:100]]

        # Get current position z-scores, and decide if they need to be closed
        for pair in self._portfolio.pairs.keys():
            if pair not in top_pairs:
                close_pairs.append(pair)

        # Close positions
        self._portfolio.close_pairs(list(set(close_pairs)))
        return
