import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.constructor.portfolio import Portfolio

from ram.strategy.long_pead.constructor.utils import ern_date_blackout
from ram.strategy.long_pead.constructor.utils import ern_price_anchor
from ram.strategy.long_pead.constructor.utils import anchor_returns


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
            'logistic_spread': [3, 4, 5, 6, 7, 8],
            'min_move': [0.06, 0.08, 0.10]
        }

    def get_daily_pl(self, arg_index, logistic_spread, min_move):
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
            momentum_rets = self.momentum_rets_dict[date]
            # Get PL
            portfolio.update_prices(closes, dividends, splits)
            portfolio.update_position_sizes(
                self._get_position_sizes(momentum_rets,
                                         logistic_spread,
                                         min_move,
                                         self.booksize))
            daily_df.loc[date, 'PL'] = portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = portfolio.get_exposure()
            exps = np.array([x.exposure for x in portfolio.positions.values()])
            daily_df.loc[date, 'Count'] = (exps != 0).sum()
        return daily_df

    def _get_position_sizes(self, mrets, logistic_spread, min_move, booksize):
        mrets = pd.Series(mrets).to_frame()
        mrets.columns = ['MomRet']
        mrets.MomRet[np.abs(mrets.MomRet) < min_move] = np.nan
        mrets = mrets.sort_values('MomRet')
        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1
        n_good = (~mrets.MomRet.isnull()).sum()
        n_bad = mrets.MomRet.isnull().sum()
        mrets['weights'] = [logistic_weight(x) for x in
                   np.linspace(-logistic_spread, logistic_spread, n_good)] + [0] * n_bad
        mrets.weights = mrets.weights / mrets.weights.abs().sum() * booksize
        return mrets.weights.to_dict()

    # ~~~~~~ Data Format ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_and_prep_data(self, data, time_index):
        formatted_data = self._format_data(data).copy()
        self.iter_dates = formatted_data['iter_dates']
        self.close_dict = formatted_data['closes']
        self.dividend_dict = formatted_data['dividends']
        self.split_mult_dict = formatted_data['split_mult']
        self.momentum_rets_dict = formatted_data['momentum_rets']

    def _format_data(self, data):
        # Anchor prices
        data = ern_date_blackout(data, offset1=-1, offset2=2)
        data = ern_price_anchor(data, offset=1)
        data = anchor_returns(data)
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
        momentum_rets = data.pivot(index='Date', columns='SecCode',
                                   values='anchor_ret').loc[test_dates]
        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        split_mult = data.pivot(
            index='Date', columns='SecCode',
            values='SplitMultiplier').fillna(1).loc[test_dates]
        return {
            'iter_dates': iter_dates,
            'closes': closes.T.to_dict(),
            'dividends': dividends.T.to_dict(),
            'split_mult': split_mult.T.to_dict(),
            'momentum_rets': momentum_rets.T.to_dict()
        }
