
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import create_time_index
from ram.strategy.sandbox.base.features import two_var_signal
from ram.strategy.sandbox.base.portfolio import Portfolio
from ram.strategy.sandbox.base.base_constructor import Constructor


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'param': [1]
            }

    def get_daily_pl(self, data_container, signals, param):
        """
        Parameters
        ----------
        data_container
        signals
        kwargs
        """
        signal_df = signals.signals.copy()
        scores_dict = make_scores_dict(signal_df)

        portfolio = Portfolio()
        test_dates = data_container.test_dates

        # Output object
        stats = {}
        daily_df = pd.DataFrame(index=test_dates.Date, dtype=float)

        if np.abs(signal_df.signal).sum() == 0:
            first_dt =  test_dates.Date.iloc[0]
            daily_df = self.update_daily_df(daily_df, portfolio, first_dt)
            daily_df.fillna(0, inplace=True)
            return daily_df, stats

        for date in test_dates.Date.values:

            vwaps, closes, dividends, splits = \
                    data_container.get_pricing_dicts(date)

            if date == test_dates.Date.iloc[0]:
                portfolio.update_prices(closes)

            if date == test_dates.Date.iloc[-1]:
                portfolio.update_splits_dividends(splits, dividends)
                portfolio.update_prices(closes)
                portfolio.close_portfolio_positions()
            else:
                portfolio.update_splits_dividends(splits, dividends)
                scores = get_scores(scores_dict, date)
                positions, net_exposure = self.get_position_sizes(scores,
                                                                 portfolio)
                position_sizes = self._get_position_sizes_dollars(positions)
                portfolio.update_position_sizes(position_sizes, vwaps)

                portfolio.update_prices(closes)

            daily_df = self.update_daily_df(daily_df, portfolio, date)
            portfolio.reset_daily_pl_turnover()

        return daily_df, stats

    def get_position_sizes(self, scores, portfolio, max_pos=.10):

        """
        Position sizes are even across positions for the day and determined
        based on the total number of positions for that day.
        """
        weights = portfolio.get_position_weights()
        prev_longs = set(weights[weights > 0].index)
        prev_shorts = set(weights[weights < 0].index)
        new_longs = set(scores[scores.weight > 0].index)
        new_shorts = set(scores[scores.weight < 0].index)

        # DO NOT UPDATE POSITION IF ALREADY ON SAME SIDE
        new_longs -= prev_longs
        new_shorts -= prev_shorts

        close_longs = set(scores[scores.weight == 0].index).intersection(prev_longs)
        close_shorts = set(scores[scores.weight == 0].index).intersection(prev_shorts)
        close_seccodes = close_longs.union(close_shorts)

        weights[new_longs] = 1
        weights[new_shorts] = -1
        weights[close_seccodes] = 0.

        openclose_ids = new_longs.union(new_shorts).union(close_seccodes)
        portfolio.update_position_weights(weights[openclose_ids].to_dict())

        if np.abs(weights).sum() != 0:
            pos_size = (1./np.abs(weights).sum())
            weights *= pos_size if pos_size < max_pos else max_pos

        net_exposure = weights.sum()
        ids_to_trade = close_seccodes.union(set(weights[weights!=0].index))

        return weights[ids_to_trade], net_exposure

    def update_daily_df(self, data, portfolio, date):
        daily_df = data.copy()
        pl_long, pl_short, daily_turnover, gross_exposure, \
                    net_exposure, weights = portfolio.get_daily_df_data()
        daily_df.loc[date, 'PL'] = pl_long + pl_short
        daily_df.loc[date, 'LongPL'] = pl_long
        daily_df.loc[date, 'ShortPL'] = pl_short
        daily_df.loc[date, 'Turnover'] = daily_turnover
        daily_df.loc[date, 'GrossExposure'] = gross_exposure
        daily_df.loc[date, 'NetExposure'] = net_exposure
        daily_df.loc[date, 'OpenLongPositions'] = (weights > 0).sum()
        daily_df.loc[date, 'OpenShortPositions'] = (weights < 0).sum()

        return daily_df

def make_scores_dict(signal_df):
    scores_dict = {}

    for date, preds in signal_df.groupby('Date'):
        scores_dict[date] = {sc:sig for sc,sig in
                             preds[['SecCode', 'signal']].values}

    return scores_dict

def get_scores(scores_dict, date):

    if date not in scores_dict.keys():
        return pd.DataFrame(columns=['score', 'weight'])

    scores =  pd.Series(scores_dict[date], name='score').to_frame()
    scores['weight'] = scores.score
    return scores[['score', 'weight']]
