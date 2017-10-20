
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.utils import make_variable_dict
from ram.strategy.starmine.constructor.portfolio import Portfolio
from ram.strategy.starmine.constructor.base_constructor import Constructor


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'long_thresh': [.02, .03, .04],
            'short_thresh': [.01, .015, .02],
            'pos_size': [.025, .035],
            'entry_dates': [[3, 4], [2, 3, 4]],
            'dd_thresh': [-99, -0.15],
            'dd_from_zero': [True],
            'close_out': [True, False]
        }

    def get_daily_pl(self, data_container, signals, entry_dates, dd_thresh,
                     dd_from_zero, long_thresh, short_thresh, **kwargs):
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
        exit_dict = data_container.exit_dict[max(entry_dates)]
        ind_groups = data_container.ind_groups

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
                                    short_thresh)
                close_seccodes = get_closing_seccodes(exit_dict, date)
                dd_seccodes = portfolio.dd_filter(dd_thresh, dd_from_zero)
                close_seccodes.update(dd_seccodes)
                close_seccodes.discard('spy')

                positions, net_exposure = self.get_position_sizes(
                    scores, portfolio, close_seccodes, **kwargs)
                position_sizes = self._get_position_sizes_dollars(positions)

                portfolio.update_position_sizes(position_sizes, vwaps)
                portfolio.update_prices(closes, dividends, splits)

                mkt_size = self._get_position_sizes_dollars(
                    {'spy':-net_exposure})
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

        weights = portfolio.get_position_weights(exclude_spy=True)
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
    
        daily_df.loc[date, 'PL'] = pl_long + pl_short
        daily_df.loc[date, 'LongPL'] = pl_long
        daily_df.loc[date, 'ShortPL'] = pl_short
        daily_df.loc[date, 'Turnover'] = daily_turnover
        daily_df.loc[date, 'Exposure'] = daily_exposure
        daily_df.loc[date, 'OpenPositions'] = sum([
            1 if x.shares != 0 else 0
            for x in portfolio.positions.values()])
        # Daily portfolio stats
        if ind_stats:
            daily_stats = portfolio.get_portfolio_stats()
            for key, value in daily_stats.iteritems():
                daily_df.loc[date, key] = value
        return daily_df

def make_scores_dict(preds_dict, entry_dates):
    scores_dict = {}
    preds_df = pd.DataFrame([])
    assert set(entry_dates).issubset(preds_dict.keys())

    for e in entry_dates:
        preds_df = preds_df.append(preds_dict[e])

    for date, preds in preds_df.groupby('Date'):
        scores_dict[date] = {s:p for s,p in preds[['SecCode', 'preds']].values}

    return scores_dict

def get_scores(scores_dict, date, long_thresh, short_thresh):

    if date not in scores_dict.keys():
        return pd.DataFrame(columns=['score', 'weight'])

    scores =  pd.Series(scores_dict[date], name='score').to_frame()
    # Scaling logic/multiple thresh vals can be handled here
    #scores['weight'] = np.where(scores.score >= long_thresh, 1.,
    #                        np.where(scores.score <= -short_thresh, -1., 0.))
    #
    scores['thresh'] = np.where(scores.score >= 0, long_thresh, -short_thresh)
    scores['weight'] = np.round(scores.score / scores.thresh, 2)
    scores.loc[scores.weight < 1, 'weight'] = 0.
    scores.loc[scores.weight > 2, 'weight'] = 2.
    scores.weight *= np.sign(scores.thresh)

    return scores[['score', 'weight']]

def get_closing_seccodes(exit_dict, date):
    if date not in exit_dict.keys():
        return set()
    else:
        return set(exit_dict[date])

