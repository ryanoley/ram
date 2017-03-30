import logging
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio
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

    def get_iterable_args(self):
        return {
            'n_pairs': [100, 300],
            'max_pos_prop': [0.04, 0.08],
            'pos_perc_deviation': [0.14],
            'z_exit': [1],
            'remove_earnings': [True],      # Definitely helpful
            'max_holding_days': [10, 30],
            'stop_perc': [-0.03, -0.10]
        }

    def get_feature_names(self):
        """
        The columns from the database that are required.
        """
        return ['AdjClose', 'RClose', 'RCashDividend',
                'GSECTOR', 'SplitFactor', 'EARNINGSFLAG']

    def get_daily_pl(self, n_pairs, max_pos_prop, pos_perc_deviation, z_exit,
                     remove_earnings, max_holding_days, stop_perc):
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
        z_exit : numeric
            At what point does one exit the position
        """
        # New portfolio created for each
        self._portfolio = PairPortfolio()

        # Output object
        daily_df = pd.DataFrame(index=self.all_dates,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        self._make_earnings_binaries()

        for date in self.all_dates:

            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            ern_flags = self.earnings_flags[date]

            exit_scores = self.exit_scores[date]
            enter_scores = self.enter_scores[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            self._portfolio.update_prices(closes, dividends, splits)

            # 2. CLOSE PAIRS
            #  Closed pairs are still in portfolio dictionary
            #  and must be cleaned at end
            self._close_signals(exit_scores, z_exit,
                                ern_flags, remove_earnings,
                                max_holding_days, stop_perc)

            # 3. ADJUST POSITIONS
            #  When the exposures move drastically (say when the markets)
            #  go up or down, it affects the size of the new positions
            #  quite significantly
            self._adjust_open_positions(n_pairs, pos_perc_deviation)

            # 4. OPEN NEW PAIRS - Just not last day of periodn
            if date != self.all_dates[-1]:
                self._execute_open_signals(enter_scores, closes,
                                           n_pairs, max_pos_prop, z_exit,
                                           ern_flags, remove_earnings)

            # Report PL and Exposure
            daily_df.loc[date, 'PL'] = self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self._portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        self._portfolio.close_all_pairs()
        daily_df.loc[date, 'PL'] += self._portfolio.get_portfolio_daily_pl()
        daily_df.loc[date, 'Exposure'] = 0

        # Shift because with executing on Close prices should consider
        # yesterday's EOD exposure
        daily_df['Ret'] = daily_df.PL / daily_df.Exposure.shift(1)
        daily_df.Ret.iloc[0] = daily_df.PL.iloc[0] / \
            daily_df.Exposure.iloc[0]

        if np.any(daily_df.Exposure > (self.booksize * 1.1)):
            logging.warn('Exposure exceeded 10% limit')

        return daily_df.loc[:, ['Ret']], self._portfolio.get_period_stats()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _close_signals(self, scores, z_exit, ern_flags, remove_earnings,
                       max_holding_days, stop_perc):

        close_pairs = []
        # Get current position z-scores, and decide if they need to be closed
        for pair in self._portfolio.pairs.keys():
            if np.abs(scores[pair]) < z_exit or np.isnan(scores[pair]):
                close_pairs.append(pair)
            # EARNINGS
            p1, p2 = pair.split('~')
            if (ern_flags[p1] or ern_flags[p2]) and remove_earnings:
                close_pairs.append(pair)
            if self._portfolio.pairs[pair].stat_holding_days >= \
                    max_holding_days:
                close_pairs.append(pair)
            # STOPS 
            ret = (self._portfolio.pairs[pair].total_pl /
                   self._portfolio.pairs[pair].entry_exposure)
            if ret < stop_perc:
                close_pairs.append(pair)
        # Close positions
        self._portfolio.close_pairs(list(set(close_pairs)))
        return

    def _adjust_open_positions(self, n_pairs, pos_perc_deviation=0.03):
        base_exposure = self.booksize / n_pairs
        self._portfolio.update_position_exposures(base_exposure,
                                                  pos_perc_deviation)

    def _execute_open_signals(self, scores, trade_prices,
                              n_pairs, max_pos_prop, z_exit,
                              ern_flags, remove_earnings):
        """
        Function that adds new positions.
        """
        open_pairs = self._portfolio.get_open_positions()
        closed_pairs = self._portfolio.get_closed_positions()
        # Drop open and closing pairs from scores
        no_go_pairs = sum([open_pairs, closed_pairs], [])
        # Get new pairs needed
        new_pairs = max(n_pairs - len(open_pairs), 0)
        if new_pairs == 0:
            return
        gross_bet_size = self.booksize / n_pairs
        # Filter
        max_pos_count = int(n_pairs * max_pos_prop)
        self._get_pos_exposures()

        for sc, pair, side in scores:
            if pair in no_go_pairs:
                continue
            if sc < (z_exit * 1.2):
                break
            leg1, leg2 = pair.split('~')
            if (ern_flags[leg1] or ern_flags[leg2]) and remove_earnings:
                continue
            if self._check_pos_exposures(leg1, leg2, side, max_pos_count):
                self._portfolio.add_pair(pair, trade_prices,
                                         gross_bet_size, side)
                new_pairs -= 1
            if new_pairs == 0:
                break
        return

    def _get_pos_exposures(self):
        """
        Get exposures each iteration.
        """
        self._exposures = {}
        for pos in self._portfolio.pairs.values():
            self._exposures[pos.leg1] = 0
            self._exposures[pos.leg2] = 0
        for pos in self._portfolio.pairs.values():
            if pos.open_position:
                self._exposures[pos.leg1] += 1 if pos.shares1 > 0 else -1
                self._exposures[pos.leg2] += 1 if pos.shares2 > 0 else -1

    def _check_pos_exposures(self, leg1, leg2, side, max_pos_count):
        """
        Returns true or false if the max position count has been hit.
        Side = 1 referse to going long the first pair, and short the second
        """
        if leg1 not in self._exposures:
            self._exposures[leg1] = 0
        if leg2 not in self._exposures:
            self._exposures[leg2] = 0
        exp1 = self._exposures[leg1] + side
        exp2 = self._exposures[leg2] - side
        if abs(exp1) <= max_pos_count:
            if abs(exp2) <= max_pos_count:
                self._exposures[leg1] += side
                self._exposures[leg2] -= side
                return True
            else:
                return False
            return False

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _make_earnings_binaries(self):
        # Add earnings flag to day before and day of to avoid entering
        # until T+1
        shift_seccode = self.data.SecCode.shift(-1)
        shift_ern_flag = self.data.EARNINGSFLAG.shift(-1)
        self.data['ERNFLAG'] = self.data.EARNINGSFLAG
        self.data.ERNFLAG += np.where(
            self.data.SecCode == shift_seccode, 1, 0) * shift_ern_flag
        self.earnings_flags = self.data.pivot(
            index='Date', columns='SecCode',
            values='ERNFLAG').fillna(0).T.astype(int).to_dict()
        self.data = self.data.drop(['ERNFLAG'], axis=1)
