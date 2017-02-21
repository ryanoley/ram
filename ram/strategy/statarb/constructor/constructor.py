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

    def get_feature_names(self):
        """
        The columns from the database that are required.
        """
        return ['AdjClose', 'RClose', 'RCashDividend',
                'GSECTOR', 'SplitFactor']

    def set_and_prep_data(self, scores, pair_info, data):
        # Trim data
        data = data[data.Date.isin(scores.index)].copy()

        self.close_dict = data.pivot(
            index='Date', columns='SecCode', values='RClose').T.to_dict()

        self.dividend_dict = data.pivot(
            index='Date', columns='SecCode',
            values='RCashDividend').fillna(0).T.to_dict()

        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        self.split_mult_dict = data.pivot(
            index='Date', columns='SecCode',
            values='SplitMultiplier').fillna(1).T.to_dict()

        # Need all scores for exit
        self.exit_scores = scores.T.to_dict()

        scores = scores.unstack().reset_index()
        scores.columns = ['Pair', 'Date', 'score']
        scores['abs_score'] = scores.score.abs()
        scores['side'] = np.where(scores.score <= 0, 1, -1)
        scores = scores.sort_values(['Date', 'abs_score'], ascending=False)
        scores['deliverable'] = zip(scores.abs_score, scores.Pair, scores.side)
        self.enter_scores = {}
        for d in np.unique(self.exit_scores.keys()):
            self.enter_scores[d] = scores.deliverable[scores.Date == d].values

        self.all_dates = np.unique(self.exit_scores.keys())
        self.pair_info = pair_info

    def get_daily_pl(self, n_pairs, max_pos_prop, pos_perc_deviation, z_exit):
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
            self._close_signals(exit_scores, z_exit)

            # 3. ADJUST POSITIONS
            #  When the exposures move drastically (say when the markets)
            #  go up or down, it affects the size of the new positions
            #  quite significantly
            self._adjust_open_positions(n_pairs, pos_perc_deviation)

            # 4. OPEN NEW PAIRS - Just not last day of periodn
            if date != self.all_dates[-1]:
                self._execute_open_signals(enter_scores, closes,
                                           n_pairs, max_pos_prop, z_exit)

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

    def _close_signals(self, scores, z_exit=1):
        close_pairs = []

        # ~~ ADD LOGIC HERE FOR CLOSING WILD MOVERS ~~ #

        # Get current position z-scores, and decide if they need to be closed
        for pair in self._portfolio.pairs.keys():
            if np.abs(scores[pair]) < z_exit or np.isnan(scores[pair]):
                close_pairs.append(pair)

        # Close positions
        self._portfolio.close_pairs(list(set(close_pairs)))
        return

    def _adjust_open_positions(self, n_pairs, pos_perc_deviation=0.03):
        base_exposure = self.booksize / n_pairs
        self._portfolio.update_position_exposures(base_exposure,
                                                  pos_perc_deviation)

    def _execute_open_signals(self, scores, trade_prices,
                              n_pairs, max_pos_prop, z_exit):
        """
        Function that adds new positions.
        """
        open_pairs = self._portfolio.get_open_positions()
        new_pairs = max(n_pairs - len(open_pairs), 0)
        if new_pairs == 0:
            return

        gross_bet_size = self.booksize / n_pairs

        assert max_pos_prop > 0 and max_pos_prop <= 1
        max_pos_count = int(n_pairs * max_pos_prop)
        self._get_pos_exposures()

        for sc, pair, side in scores:
            if pair in open_pairs:
                continue
            if sc < (z_exit * 1.2):
                break
            if self._check_pos_exposures(pair, side, max_pos_count):
                self._portfolio.add_pair(pair, trade_prices,
                                         gross_bet_size, side)
            if self._portfolio.count_open_positions() == n_pairs:
                break
        return

    def _get_pos_exposures(self):
        """
        Get exposures each iteration.
        """
        self._exposures = {}
        for _, pos in self._portfolio.pairs.iteritems():
            if pos.open_position:
                for leg, shares in zip(pos.legs, pos.shares):
                    if leg not in self._exposures:
                        self._exposures[leg] = 0
                    self._exposures[leg] += np.sign(shares)

    def _check_pos_exposures(self, pair, side, max_pos_count):
        side1, side2 = pair.split('~')
        legs1 = side1.split('_')
        legs2 = side2.split('_')
        legs = np.append(legs1, legs2)
        sides = np.append(np.repeat(side, len(legs1)),
                          np.repeat(-side, len(legs2)))
        for leg in legs:
            if leg not in self._exposures:
                self._exposures[leg] = 0
        exps = np.array([self._exposures[l] for l in legs])
        if np.any(np.abs(exps + sides) == max_pos_count):
            return False
        else:
            for leg, side in zip(legs, sides):
                self._exposures[leg] += side
            return True
