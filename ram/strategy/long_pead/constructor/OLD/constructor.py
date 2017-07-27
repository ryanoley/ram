"""
This was the original idea where you blackout around earnings announcements
and also have an anchor price that adjusts as time moves forward.
"""
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.constructor.portfolio import Portfolio

from ram.strategy.long_pead.constructor.utils import ern_date_blackout
from ram.strategy.long_pead.constructor.utils import ern_price_anchor


class PortfolioConstructor(object):

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize
        self._portfolios = {}
        self._data = {}

    def get_iterable_args(self):
        return {
            'logistic_spread': [.1, .5, 1, 2]
        }

    def get_data_args(self):
        return {
            'blackout_offset1': [-1],
            'blackout_offset2': [3, 5, 6, 7, 8],
            'anchor_init_offset': [5, 6, 7, 8],
            'anchor_window': [15, 25]
        }

    def get_daily_pl(self, arg_index, logistic_spread):
        """
        Parameters
        ----------
        """
        portfolio = Portfolio()
        # Output object
        daily_df = pd.DataFrame(index=self.iter_dates,
                                columns=['PL', 'Exposure', 'Count'],
                                dtype=float)
        for date in self.iter_dates:
            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            anchor_rets = self.anchor_rets_dict[date]
            # Get PL
            portfolio.update_prices(closes, dividends, splits)
            portfolio.update_position_sizes(
                self._get_position_sizes(anchor_rets,
                                         logistic_spread,
                                         self.booksize), closes)
            daily_df.loc[date, 'PL'] = portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = portfolio.get_portfolio_exposure()
            exps = np.array([x.exposure for x in portfolio.positions.values()])
            daily_df.loc[date, 'Count'] = (exps != 0).sum()
        return daily_df

    def _get_position_sizes(self, mrets, logistic_spread, booksize):
        mrets = pd.Series(mrets).to_frame()
        mrets.columns = ['MomRet']
        mrets = mrets.sort_values('MomRet')
        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1
        n_good = (~mrets.MomRet.isnull()).sum()
        n_bad = mrets.MomRet.isnull().sum()
        mrets['weights'] = [
            logistic_weight(x) for x in np.linspace(
                logistic_spread, -logistic_spread, n_good)] + [0] * n_bad
        mrets.weights = mrets.weights / mrets.weights.abs().sum() * booksize
        return mrets.weights.to_dict()

    # ~~~~~~ Data Format ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_and_prep_data(self, data, time_index,
                          blackout_offset1,
                          blackout_offset2,
                          anchor_init_offset,
                          anchor_window):
        # Anchor prices
        data = ern_date_blackout(data, offset1=blackout_offset1,
                                 offset2=blackout_offset2)
        data = ern_price_anchor(data, init_offset=anchor_init_offset,
                                window=anchor_window)

        # Get training and test dates
        test_dates = data[data.TestFlag].Date.drop_duplicates()
        qtrs = np.array([(x.month-1)/3+1 for x in test_dates])
        iter_dates = test_dates[qtrs == qtrs[0]]
        # Calculate State Variables, including Z-Score
        closes = data.pivot(
            index='Date', columns='SecCode', values='RClose').loc[test_dates]
        dividends = data.pivot(
            index='Date', columns='SecCode',
            values='RCashDividend').fillna(0).loc[test_dates]
        anchor_rets = data.pivot(index='Date', columns='SecCode',
                                 values='anchor_ret').loc[test_dates]
        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        split_mult = data.pivot(
            index='Date', columns='SecCode',
            values='SplitMultiplier').fillna(1).loc[test_dates]
        self.iter_dates = iter_dates
        self.close_dict = closes.T.to_dict()
        self.dividend_dict = dividends.T.to_dict()
        self.split_mult_dict = split_mult.T.to_dict()
        self.anchor_rets_dict = anchor_rets.T.to_dict()
