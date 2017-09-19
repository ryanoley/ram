
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.constructor.portfolio import Portfolio
from ram.strategy.starmine.constructor.base_constructor import Constructor
from ram.strategy.starmine.utils import make_variable_dict, get_prior_business_date


COST = .0015


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'thresh': [0.005, 0.01],
            'pos_size': [.002, .0035],
            'dd_thresh': [-.05, -.1],
        }

    def get_daily_pl(self, data_container, signals, **kwargs):
        """
        Parameters
        ----------
        data_container
        signals
        kwargs
        """
        scores_dict = make_variable_dict(signals.preds_data, 'preds')
        exit_dict = data_container.exit_dict.copy()

        portfolio = Portfolio()
        test_dates = data_container.test_dates
        prior_bdates = get_prior_business_date(test_dates)

        # Output object
        daily_df = pd.DataFrame(index=test_dates, dtype=float)

        for prior_dt, date in zip(prior_bdates, test_dates):

            vwaps, closes, dividends, splits = \
                    data_container.get_pricing_dicts(date)

            mkt_vwap, mkt_close, mkt_dividend, mkt_split = \
                    data_container.get_pricing_dicts(date, mkt_prices=True)

            if date == test_dates[0]:
                portfolio.update_prices(closes, dividends, splits)
                portfolio.update_prices(mkt_close, mkt_dividend, mkt_split)
            elif date == test_dates[-1]:
                portfolio.close_portfolio_positions()
            else:
                scores = scores_dict[prior_dt]
                close_seccodes = self.get_closing_seccodes(exit_dict, date)

                positions, net_exposure = self.get_position_sizes(
                    scores, portfolio, close_seccodes, **kwargs)

                position_sizes = self._get_position_sizes_dollars(positions)
                portfolio.update_position_sizes(position_sizes, vwaps)
                portfolio.update_prices(closes, dividends, splits)

                mkt_size = self._get_position_sizes_dollars(
                    {'spy':-net_exposure})
                portfolio.update_position_sizes(mkt_size, mkt_vwap)
                portfolio.update_prices(mkt_close, mkt_dividend, mkt_split)

            daily_df = self.update_daily_df(daily_df, portfolio, date)

        # Time Index aggregate stats
        stats = {}
        return daily_df, stats

    def get_closing_seccodes(self, exit_dict, date):
        if date not in exit_dict.keys():
            return set()
        else:
            return set(exit_dict[date])

    def get_position_sizes(self, scores, portfolio, close_seccodes, thresh,
                           pos_size, dd_thresh):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.
        """

        scores = pd.Series(scores, name='score').to_frame()
        new_longs = set(scores[scores.score >= thresh].index)
        new_shorts = set(scores[scores.score <= -thresh].index)

        dd_seccodes = portfolio.dd_filter(drawdown_pct = dd_thresh)
        prev_longs, prev_shorts = portfolio.get_open_positions()

        live_shorts = (prev_shorts - close_seccodes) - dd_seccodes
        live_shorts.update(new_shorts)

        live_longs = (prev_longs - close_seccodes) - dd_seccodes
        live_longs.update(new_longs)

        scores.loc[:, 'weights'] = 0.
        scores.loc[scores.index.isin(live_longs), 'weights'] = 1.
        scores.loc[scores.index.isin(live_shorts), 'weights'] = -1.

        exposure = pos_size * (np.abs(scores.weights).sum() +
                               np.abs(scores.weights.sum()))
        scaled_size = (1. / exposure) * pos_size if exposure > 1. else pos_size
        scores['weights'] *= scaled_size
        net_exposure = scores.weights.sum()

        ids_to_trade = close_seccodes.copy()
        ids_to_trade.update(dd_seccodes)
        if scaled_size == pos_size:
            ids_to_trade.update(new_longs)
            ids_to_trade.update(new_shorts)
        else:
            ids_to_trade.update(live_longs)
            ids_to_trade.update(live_shorts)

        scores = scores[scores.index.isin(ids_to_trade)]

        return pd.Series(scores.weights), net_exposure


    def update_daily_df(self, data, portfolio, date):
        daily_df = data.copy()
        pl_long, pl_short = portfolio.get_portfolio_daily_pl()
        daily_turnover = portfolio.get_portfolio_daily_turnover()
        daily_exposure = portfolio.get_portfolio_exposure()
    
        daily_df.loc[date, 'PL'] = pl_long + pl_short
        daily_df.loc[date, 'LongPL'] = pl_long
        daily_df.loc[date, 'ShortPL'] = pl_short
        daily_df.loc[date, 'Turnover'] = daily_turnover
        daily_df.loc[date, 'Exposure'] = daily_exposure
        daily_df.loc[date, 'OpenPositions'] = sum([
            1 if x.shares != 0 else 0
            for x in portfolio.positions.values()])
        # Daily portfolio stats
        daily_stats = portfolio.get_portfolio_stats()
        daily_df.loc[date, 'stat1'] = daily_stats['stat1']
        return daily_df

