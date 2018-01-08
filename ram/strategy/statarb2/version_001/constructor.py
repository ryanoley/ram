import numba
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.utils import make_arg_iter
from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb.abstract.portfolio import Portfolio

LOW_PRICE_FILTER = 7
LOW_LIQUIDITY_FILTER = 3
BOOKSIZE = 2e6


class PortfolioConstructor(object):

    def get_args(self):
        return make_arg_iter({
            'prma_x': [5, 10, 15, 20],
            'split_perc': [20, 40],
        })

    def set_args(self, prma_x, split_perc):
        self._prma_x = prma_x
        self._split_perc = split_perc

    def process(self, train_data, test_data):

        portfolio = Portfolio()

        scores = make_variable_dict(test_data,
                                    'prma_{}'.format(self._prma_x))
        closes = make_variable_dict(test_data, 'RClose')
        dividends = make_variable_dict(test_data, 'RCashDividend', 0)
        splits = make_variable_dict(test_data, 'SplitMultiplier', 1)
        liquidity = make_variable_dict(test_data, 'AvgDolVol')
        market_cap = make_variable_dict(test_data, 'MarketCap')

        # Dates to iterate over - just one month plus one day
        unique_test_dates = np.unique(test_data.Date)
        months = np.diff([x.month for x in unique_test_dates])
        change_ind = np.where(months)[0][0] + 2
        unique_test_dates = unique_test_dates[:change_ind]

        # Output object
        daily_df = pd.DataFrame(index=unique_test_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for i, date in enumerate(unique_test_dates):

            # If a low liquidity/price value, set score to nan
            # Update every five days
            if i % 5 == 0:
                low_liquidity_seccodes = filter_seccodes(
                    liquidity[date], LOW_LIQUIDITY_FILTER)
                low_price_seccodes = filter_seccodes(
                    closes[date], LOW_PRICE_FILTER)

            for seccode in set(low_liquidity_seccodes+low_price_seccodes):
                scores[date][seccode] = np.nan

            portfolio.update_prices(
                closes[date], dividends[date], splits[date])

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()

            elif i % 5 == 0:
                sizes = self.get_day_position_sizes(date, scores[date])
                portfolio.update_position_sizes(sizes, closes[date])

            pl_long, pl_short = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            daily_df.loc[date, 'PL'] = (pl_long + pl_short) / BOOKSIZE
            daily_df.loc[date, 'LongPL'] = pl_long / BOOKSIZE
            daily_df.loc[date, 'ShortPL'] = pl_short / BOOKSIZE
            daily_df.loc[date, 'Turnover'] = daily_turnover / BOOKSIZE
            daily_df.loc[date, 'Exposure'] = daily_exposure
            daily_df.loc[date, 'OpenPositions'] = sum([
                1 if x.shares != 0 else 0
                for x in portfolio.positions.values()])

        return daily_df

    def get_day_position_sizes(self, date, scores):
        scores = pd.Series(scores).to_frame()
        scores.columns = ['score']
        long_thresh = np.percentile(scores.score.dropna(), self._split_perc)
        short_thresh = np.percentile(scores.score.dropna(), 100 - self._split_perc)
        scores['alloc'] = np.where(scores.score < long_thresh, 1,
                          np.where(scores.score > short_thresh, -1, 0))
        scores.alloc = scores.alloc / scores.alloc.abs().sum() * BOOKSIZE

        return scores.alloc.to_dict()


def filter_seccodes(data_dict, min_value):
    bad_seccodes = []
    for key, value in data_dict.iteritems():
        if value < min_value:
            bad_seccodes.append(key)
    return bad_seccodes
