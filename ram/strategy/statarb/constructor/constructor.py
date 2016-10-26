import datetime as dt
import numpy as np
import pandas as pd

from ram.strategy.statarb.portfolio import PairPortfolio
from ram.strategy.statarb.constructor.base import BaseConstructor


class PortfolioConstructor(BaseConstructor):

    def __init__(self):
        self.portfolio = PairPortfolio()

    def get_daily_pl(self, scores, booksize,
                     Close, Dividend, SplitMultiplier, **kwargs):

        # Map close price Tickers for faster updating
        self.portfolio.map_close_id_index(Close)

        # Output object
        daily_df = pd.DataFrame(index=Close.index,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        for date in Close.index:

            # Get current period data
            cl = Close.loc[date]
            div = Dividend.loc[date]
            spl = SplitMultiplier.loc[date]
            sc = scores.loc[date]

            # Update all the prices in portfolio. This calculates PL
            # for individual positions
            self.portfolio.update_prices(cl, div, spl)

            # 1. CLOSE PAIRS IF NEEDED
            #  Closed pairs are still in portfolio dictionary and must
            #  be cleaned at end
            self._close_signals(sc, **kwargs)

            # 2. OPEN PAIRS IF NEEDED
            # Must consider portfolio
            self._execute_open_signals(sc, cl, booksize, **kwargs)

            # Get exposure
            daily_df.loc[date, 'PL'] = self.portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = \
                self.portfolio.get_gross_exposure()

        # Clear all pairs in portfolio and adjust PL
        cost = self.portfolio.remove_pairs(all_pairs=True)
        daily_df.loc[date, 'PL'] -= cost
        daily_df.loc[date, 'Exposure'] = 0
        return daily_df

    def _close_signals(self, scores, z_exit=1, **kwargs):
        """
        Innovation can happen here
        """
        # Remove positions that have gross exposure of zero from update_prices
        close_pairs = [nm for nm, pos in self.portfolio.positions.iteritems()
                       if pos.gross_exposure == 0]
        # Get current position z-scores
        p_scores = self._position_scores(scores)
        # Remove scores that are less than abs(z_exit)
        for pair, val in p_scores.iteritems():
            if np.abs(val) < z_exit | np.isnan(val):
                close_pairs.append(pair)
        self.portfolio.remove_pairs(close_pairs)
        return

    def _position_scores(self, scores):
        p_scores = {}
        # Get current positions
        for pair in self.portfolio.positions.keys():
            p_scores[pair] = scores[pair]
        return p_scores

    def _execute_open_signals(self, scores, trade_prices, booksize,
                              n_pairs=100, max_pos_exposure=0.05,
                              min_pos_exposure=0.001, **kwargs):
        """
        Function that adds new positions.
        """
        scores = scores.dropna()
        current_pos = self.portfolio.positions.keys()
        # Count new pairs, if none, then exit
        new_pairs = n_pairs - len(current_pos)
        if new_pairs == 0:
            return
        # Remove those currently in positions
        scores = scores[~scores.index.isin(current_pos)]
        # SELECTION
        pairs, sides, bet_size = self._get_new_pairs_max_exposure(
            scores, new_pairs, max_pos_exposure, min_pos_exposure, booksize)
        # Put on new positions
        if pairs:
            self.portfolio.add_pairs(pairs, sides, trade_prices, bet_size)
        return

    def _get_new_pairs_max_exposure(self,
                                    scores, new_pairs,
                                    max_pos_exposure,
                                    min_pos_exposure,
                                    booksize):
        """
        Successively add new pairs to portfolio as long as the new position
        does add too much exposure to one symbol.
        """
        gross_exp = self.portfolio.get_gross_exposure()
        bet_size = max(booksize - gross_exp, 0)
        if bet_size == 0:
            return [], [], 0

        max_pos = max_pos_exposure * booksize
        min_pos = min_pos_exposure * booksize

        indiv_leg_size = bet_size / new_pairs / 2

        if indiv_leg_size < min_pos:
            indiv_leg_size = min_pos

        # Get max size for individual symbol
        # Current position values of all open values
        symbolvals = self.portfolio.get_symbol_values()
        # Currently how the pairs are selected
        scores = scores[np.argsort(np.abs(scores.values))][::-1]
        pairs = []
        sides = []
        for pair, sc in scores.iteritems():
            leg1, leg2 = pair.split('_')
            # Get potential position sizes. If score is above 0, then short
            # the first pair/long the second, and vica versa
            val1, val2 = (-indiv_leg_size, indiv_leg_size) if sc >= 0 \
                else (indiv_leg_size, -indiv_leg_size)

            if leg1 in symbolvals:
                if abs(symbolvals[leg1] + val1) > max_pos:
                    continue
            else:
                symbolvals[leg1] = 0

            if leg2 in symbolvals:
                if abs(symbolvals[leg2] + val2) > max_pos:
                    continue
            else:
                symbolvals[leg2] = 0

            # If here, then add position
            pairs.append(pair)
            sides.append(-1 if sc >= 0 else 1)
            symbolvals[leg1] += val1
            symbolvals[leg2] += val2
            gross_exp += abs(val1) + abs(val2)
            if gross_exp >= booksize:
                break
        return pairs, sides, bet_size
