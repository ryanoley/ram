
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.utils import make_variable_dict
from ram.strategy.starmine.constructor.portfolio import Portfolio
from ram.strategy.starmine.constructor.base_constructor import Constructor


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'long_thresh': [.025, .03, .035],
            'short_thresh': [.005, .0075, .01],
            'pos_size': [.02],
            'entry_dates': [[2, 3, 4, 5]],
            'dd_thresh': [-.2, -.1],
            'close_out': [True],
            'scale_weights': [True]
        }

    def get_daily_pl(self, data_container, signals, entry_dates, dd_thresh,
                     long_thresh, short_thresh, scale_weights, **kwargs):
        """
        Parameters
        ----------
        data_container
        signals
        kwargs
        """
        scores_dict = make_scores_dict(signals.preds_data, entry_dates)
        portfolio = Portfolio()

        test_dates = data_container.test_dates
        ind_groups = data_container.ind_groups
        hold_per = data_container.hold_per

        # Output object
        daily_df = pd.DataFrame(index=test_dates.Date, dtype=float)

        for date, prior_dt in test_dates.values:

            vwaps, closes, dividends, splits = \
                    data_container.get_pricing_dicts(date)

            mkt_vwap, mkt_close, mkt_adj_close, mkt_dividend, mkt_split = \
                    data_container.get_pricing_dicts(date, mkt_prices=True)

            if date == test_dates.Date.iloc[0]:
                portfolio.update_prices(closes, dividends, splits)
                portfolio.update_prices(mkt_close, mkt_dividend, mkt_split)
                portfolio.add_sector_info(ind_groups)
            elif date == test_dates.Date.iloc[-1]:
                portfolio.close_portfolio_positions()
            else:
                scores = get_scores(scores_dict, prior_dt, long_thresh,
                                    short_thresh, scale_weights)
                close_seccodes = self.get_closing_seccodes(portfolio, hold_per)
                dd_seccodes = portfolio.dd_filter(dd_thresh)
                exit_flag = close_seccodes.union(dd_seccodes)
                exit_flag.discard('HEDGE')

                positions, net_exposure = self.get_position_sizes(
                    scores, portfolio, exit_flag, **kwargs)
                position_sizes = self._get_position_sizes_dollars(positions)

                portfolio.update_position_sizes(position_sizes, vwaps)
                portfolio.update_prices(closes, dividends, splits)

                mkt_size = self._get_position_sizes_dollars(
                    {'HEDGE':-net_exposure})
                portfolio.update_position_sizes(mkt_size, mkt_vwap)
                portfolio.update_prices(mkt_close, mkt_dividend, mkt_split)

                portfolio.update_mkt_prices(mkt_adj_close)

            daily_df = self.update_daily_df(daily_df, portfolio, date,
                                            ind_stats=False)
            portfolio.reset_daily_pl()

        # Time Index aggregate stats
        stats = {}
        return daily_df, stats

    def get_position_sizes(self, scores, portfolio, close_seccodes, pos_size, 
                           close_out=False):
    
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.
        """

        weights = portfolio.get_position_weights()
        prev_longs = set(weights[weights > 0].index)
        prev_shorts = set(weights[weights < 0].index)

        new_longs = set(scores[scores.weight > 0].index)
        new_shorts = set(scores[scores.weight < 0].index)
        # DO NOT UPDATE POSITION IF ALREADY ON SAME SIDE
        new_longs -= prev_longs
        new_shorts -= prev_shorts

        no_trades = set()
        if close_out:
            no_trades = set(scores[scores.weight == 0].index)
        close_seccodes.update(no_trades)

        weights[new_longs] = scores.loc[new_longs, 'weight']
        weights[new_shorts] = scores.loc[new_shorts, 'weight']
        weights[close_seccodes] = 0.
        portfolio.update_holding_days(weights[weights != 0].to_dict())

        ids_to_trade = new_longs.union(new_shorts).union(close_seccodes)
        portfolio.update_position_weights(weights[ids_to_trade].to_dict())

        exposure = pos_size * (np.abs(weights).sum() + np.abs(weights.sum()))
        scale_factor = (1. / exposure) if exposure > 1. else 1.
        positions = weights * scale_factor * pos_size
        net_exposure = positions.sum()

        if scale_factor < 1:
            ids_to_trade.update(positions[positions != 0].index)

        return positions[ids_to_trade], net_exposure

    def update_daily_df(self, data, portfolio, date, ind_stats=False):
        daily_df = data.copy()
        pl_long, pl_short = portfolio.get_portfolio_daily_pl()
        daily_turnover = portfolio.get_portfolio_daily_turnover()
        daily_exposure = portfolio.get_portfolio_exposure()
        n_longs, n_shorts = portfolio.get_portfolio_position_totals()

        daily_df.loc[date, 'PL'] = pl_long + pl_short
        daily_df.loc[date, 'LongPL'] = pl_long
        daily_df.loc[date, 'ShortPL'] = pl_short
        daily_df.loc[date, 'Turnover'] = daily_turnover
        daily_df.loc[date, 'Exposure'] = daily_exposure
        daily_df.loc[date, 'OpenLongPositions'] = n_longs
        daily_df.loc[date, 'OpenShortPositions'] = n_shorts
        # Daily portfolio stats
        if ind_stats:
            daily_stats = portfolio.get_portfolio_stats()
            for key, value in daily_stats.iteritems():
                daily_df.loc[date, key] = value
        return daily_df

    def get_closing_seccodes(self, portfolio, hold_per):
        close_seccodes = set()
        for position in portfolio.positions.itervalues():
            if position.hold_days >= (hold_per - 1):
                close_seccodes.add(position.symbol)
        return close_seccodes


def make_scores_dict(preds_dict, entry_dates):
    scores_dict = {}
    preds_df = pd.DataFrame([])
    assert set(entry_dates).issubset(preds_dict.keys())

    for e in entry_dates:
        preds_df = preds_df.append(preds_dict[e])

    for date, preds in preds_df.groupby('Date'):
        scores_dict[date] = {s:p for s,p in preds[['SecCode', 'preds']].values}

    return scores_dict


def get_scores(scores_dict, date, long_thresh, short_thresh,
               scale_weights=False):

    if date not in scores_dict.keys():
        return pd.DataFrame(columns=['score', 'weight'])

    scores =  pd.Series(scores_dict[date], name='score').to_frame()
    scores['thresh'] = np.where(scores.score >= 0, long_thresh, -short_thresh)

    if scale_weights:
        scores['weight'] = np.round(scores.score / scores.thresh, 2)
        scores.loc[scores.weight < 1, 'weight'] = 0.
        scores.loc[scores.weight > 2, 'weight'] = 2.
        scores.weight *= np.sign(scores.thresh)
    else:
        scores['weight'] = 0.
        scores.loc[(scores.score <= scores.thresh) & (scores.score < 0), 'weight'] = -1
        scores.loc[(scores.score >= scores.thresh) & (scores.score > 0), 'weight'] = 1

    return scores[['score', 'weight']]

