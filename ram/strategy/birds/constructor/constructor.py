import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.birds.constructor.portfolio import Portfolio
from ram.strategy.birds.constructor.base import BaseConstructor


class PortfolioConstructor(BaseConstructor):

    bet_size = 100000

    def get_daily_pl(self):
        """
        Parameters
        ----------
        """
        features = self.signals.keys()
        # Output object
        output_df = pd.DataFrame(index=self.all_dates,
                                 columns=features,
                                 dtype=float)
        # Iterate features here
        for feature in features:

            daily_df = pd.DataFrame(index=self.all_dates,
                                    columns=['PL', 'Exposure'],
                                    dtype=float)

            # New portfolio created for each feature
            self._portfolio = Portfolio()

            for date in self.all_dates:

                closes = self.close_dict[date]
                dividends = self.dividend_dict[date]
                splits = self.split_mult_dict[date]

                signals = self.signals[feature][date]

                # 1. Update all the prices in portfolio. This calculates PL
                #    for individual positions
                self._portfolio.update_prices(closes, dividends, splits)

                # 2. Open/Close positions based on signals
                self._open_close_positions(closes, signals)

                # Rebalance anything that is open and has deviated
                self._portfolio.update_position_exposures(self.bet_size, 0.05)

                # Report PL and Exposure
                daily_df.loc[date, 'PL'] = \
                    self._portfolio.get_portfolio_daily_pl()
                daily_df.loc[date, 'Exposure'] = \
                    self._portfolio.get_gross_exposure()

            # Clear all pairs in portfolio and adjust PL
            self._portfolio.close_all_positions()
            daily_df.loc[date, 'PL'] += \
                self._portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = 0

            # Shift because with executing on Close prices should consider
            # yesterday's EOD exposure to get return
            daily_df['Ret'] = daily_df.PL / daily_df.Exposure.shift(1)
            daily_df.Ret.iloc[0] = daily_df.PL.iloc[0] / \
                daily_df.Exposure.iloc[0]

            output_df.loc[:, feature] = daily_df.Ret

        return output_df

    def _open_close_positions(self, closes, signals):
        for symbol, side in signals.iteritems():
            if symbol in self._portfolio.positions:
                pos = self._portfolio.positions[symbol]
                if side == 0:
                    pos.close_position()
                elif side != np.sign(pos.exposure):
                    pos.update_position_exposure(side * self.bet_size)
            elif side != 0:
                self._portfolio.add_position(symbol, closes,
                                             side * self.bet_size)
