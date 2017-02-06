import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio
from ram.strategy.statarb.constructor.position import PairPosition
from ram.strategy.statarb.constructor.base import BaseConstructor


class PortfolioConstructor(BaseConstructor):

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

    def get_daily_pl(self, scores, data, pair_info, n_pairs, max_pos_prop,
                     pos_perc_deviation):
        """
        Parameters
        ----------
        scores : pd.DataFrames
            Dates in index and pairs in columns
        data : pd.DataFrames
            Daily data for each individual security
        pair_info : pd.DataFrames
            Any additional information that could be used to construct
            portfolio from the pair selection process.

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
        close_table = data.pivot(index='Date',
                                 columns='SecCode',
                                 values='AdjClose')

        dividend_table = data.pivot(index='Date',
                                    columns='SecCode',
                                    values='RCashDividend').fillna(0)

        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) + 1
        split_mult_table = data.pivot(index='Date',
                                      columns='SecCode',
                                      values='SplitMultiplier').fillna(1)

        # Output object
        daily_df = pd.DataFrame(index=scores.index,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        for date in scores.index:

            # Get current period data and put into dictionary
            # for faster lookup
            closes = close_table.loc[date].to_dict()
            dividends = dividend_table.loc[date].to_dict()
            splits = split_mult_table.loc[date].to_dict()

            sc = scores.loc[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(closes, dividends, splits)

            # 2. CLOSE PAIRS
            #  Closed pairs are still in portfolio dictionary
            #  and must be cleaned at end
            self._close_signals(sc, z_exit=1)

            # 3. ADJUST POSITIONS
            #  When the exposures move drastically (say when the markets)
            #  go up or down, it affects the size of the new positions
            #  quite significantly
            self._adjust_open_positions(n_pairs, pos_perc_deviation)

            # 4. OPEN NEW PAIRS
            if date != scores.index[-1]:
                self._execute_open_signals(sc, closes, n_pairs, max_pos_prop)

            # Report PL and Exposureexposure
            daily_df.loc[date, 'PL'] = self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self._portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        self._portfolio.close_pairs(all_pairs=True)
        daily_df.loc[date, 'PL'] += self._portfolio.get_portfolio_daily_pl()
        daily_df.loc[date, 'Exposure'] = 0
        # Could this be a flag?
        daily_df['Ret'] = daily_df.PL / daily_df.Exposure
        daily_df.Ret.iloc[-1] = daily_df.PL.iloc[-1] / \
            daily_df.Exposure.iloc[-2]
        return daily_df.loc[:, ['Ret']]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _close_signals(self, scores, z_exit=1):
        close_pairs = []

        # ~~ ADD LOGIC HERE FOR CLOSING WILD MOVERS ~~ #

        # Get current position z-scores, and decide if they need to be closed
        scores2 = scores.to_dict()
        for pair in self._portfolio.pairs.keys():
            if np.abs(scores2[pair]) < z_exit | np.isnan(scores2[pair]):
                close_pairs.append(pair)

        # Close positions
        self._portfolio.close_pairs(list(set(close_pairs)))
        return

    def _adjust_open_positions(self, n_pairs, pos_perc_deviation=0.03):
        base_exposure = self.booksize / n_pairs
        self._portfolio.update_position_exposures(base_exposure,
                                                  pos_perc_deviation)

    def _execute_open_signals(self, scores, trade_prices,
                              n_pairs, max_pos_prop):
        """
        Function that adds new positions.
        """
        open_pairs = self._portfolio.get_open_positions()
        new_pairs = max(n_pairs - len(open_pairs), 0)
        if new_pairs == 0:
            return

        pair_bet_size = self.booksize / n_pairs
        scores2 = pd.DataFrame({'abs_score': scores.abs(),
                                'side': np.where(scores <= 0, 1, -1)})
        scores2 = scores2.sort_values(['abs_score'], ascending=False)

        assert max_pos_prop > 0 and max_pos_prop <= 1
        max_pos_count = int(n_pairs * max_pos_prop)
        self._get_pos_exposures()

        for pair, (sc, side) in scores2.iterrows():
            if pair in open_pairs:
                continue
            if self._check_pos_exposures(pair, side, max_pos_count):
                self._portfolio.add_pair(pair, trade_prices,
                                         pair_bet_size, side)
            if self._portfolio.count_open_positions() == n_pairs:
                break
        return

    def _get_pos_exposures(self):
        """
        Get exposures each iteration.
        """
        self._exposures = {}
        for pair, pos in self._portfolio.pairs.iteritems():
            leg1, leg2 = pair.split('_')
            if pos.open_position:
                if leg1 not in self._exposures:
                    self._exposures[leg1] = 0
                if leg2 not in self._exposures:
                    self._exposures[leg2] = 0
                side = 1 if pos.shares1 > 0 else -1
                self._exposures[leg1] += side
                self._exposures[leg2] += -1 * side

    def _check_pos_exposures(self, pair, side, max_pos_count):
        leg1, leg2 = pair.split('_')

        if leg1 not in self._exposures:
            self._exposures[leg1] = 0
        if leg2 not in self._exposures:
            self._exposures[leg2] = 0

        if abs(self._exposures[leg1]) < max_pos_count and \
                abs(self._exposures[leg2]) < max_pos_count:
            self._exposures[leg1] += side
            self._exposures[leg2] += -1 * side
            return True
        else:
            return False
