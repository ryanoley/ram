
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.starmine.utils import make_variable_dict
from ram.strategy.starmine.constructor.portfolio import Portfolio
from ram.strategy.starmine.constructor.base_constructor import Constructor


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'thresh': [.025, .035, .045],
            'pos_size': [.05, .065],
            'entry_dates': [[3, 4], [2, 3, 4]],
            'dd_thresh': [-99, -.15],
            'dd_from_zero': [True],
            'close_out': [True, False]
        }

    def get_daily_pl(self, data_container, signals, entry_dates, dd_thresh,
                     dd_from_zero, **kwargs):
        """
        Parameters
        ----------
        data_container
        signals
        kwargs
        """
        scores_dict = make_scores_dict(signals.preds_data, entry_dates)
        portfolio = Portfolio()

        test_ids = data_container.test_ids
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
                scores = get_scores(scores_dict, prior_dt, test_ids)
                close_seccodes = get_closing_seccodes(exit_dict, date)
                dd_seccodes = portfolio.dd_filter(dd_thresh, dd_from_zero)
                close_seccodes.update(dd_seccodes)

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

    def get_position_sizes(self, scores, portfolio, close_seccodes,
                           thresh, pos_size, close_out=False):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.
        """

        prev_longs, prev_shorts = portfolio.get_open_positions()

        new_univ = scores[scores.score.notnull()]
        new_longs = set(new_univ[new_univ.score >= thresh].index)
        new_shorts = set(new_univ[new_univ.score <= -thresh].index)
        if close_out:
            no_trades = set(new_univ.index) - new_longs - new_shorts
        else:
            no_trades = set()

        # Once a position is on, do not update if on same side
        new_longs -= prev_longs
        new_shorts -= prev_shorts

        close_seccodes.update(no_trades)
        live_shorts = prev_shorts - close_seccodes
        live_shorts.update(new_shorts)

        live_longs = prev_longs - close_seccodes
        live_longs.update(new_longs)

        scores['weights'] = 0.
        scores.loc[scores.index.isin(live_longs), 'weights'] = 1.
        scores.loc[scores.index.isin(live_shorts), 'weights'] = -1.

        exposure = pos_size * (np.abs(scores.weights).sum() +
                               np.abs(scores.weights.sum()))
        scaled_size = (1. / exposure) * pos_size if exposure > 1. else pos_size
        scores['weights'] *= scaled_size
        net_exposure = scores.weights.sum()

        ids_to_trade = close_seccodes.copy()
        if scaled_size == pos_size:
            ids_to_trade.update(new_longs)
            ids_to_trade.update(new_shorts)
        else:
            ids_to_trade.update(live_longs)
            ids_to_trade.update(live_shorts)

        scores = scores[scores.index.isin(ids_to_trade)]

        return pd.Series(scores.weights), net_exposure

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
        scores_dict[date] = {s:p for s,p in preds[['SecCode','preds']].values}

    return scores_dict

def get_scores(scores_dict, date, index_ids):
    score_df = pd.DataFrame(index=index_ids)

    if date in scores_dict.keys():
        score_df['score'] =  pd.Series(scores_dict[date], name='score')
    else:
        score_df['score'] = np.nan

    return score_df
    
def get_closing_seccodes(exit_dict, date):
    if date not in exit_dict.keys():
        return set()
    else:
        return set(exit_dict[date])
