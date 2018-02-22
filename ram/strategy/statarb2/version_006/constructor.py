import numba
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.utils import make_arg_iter
from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb2.portfolio import Portfolio

BOOKSIZE = 4e6


class PortfolioConstructor(object):

    def get_args(self):
        return make_arg_iter({
            'score_var': ['prma_10', 'prma_15', 'prma_20', 'ret_10d',
                          'boll_10', 'boll_20', 'rsi_15'],
            'per_side_count': [10, 20, 30],
            'holding_period': [3, 5, 7, 9]
        })

    def set_args(self,
                 score_var,
                 per_side_count,
                 holding_period):
        self._score_var = score_var
        self._per_side_count = per_side_count
        self._holding_period = holding_period

    def process(self, trade_data, signals):

        portfolio = Portfolio()

        scores = trade_data[self._score_var]
        scores.columns = ['SecCode', 'Date', 'scores']

        scores = scores.merge(signals)
        scores = scores.sort_values(['Date', 'Signal'])
        blank_allocs = {x: 0 for x in scores.SecCode.unique()}

        scores.set_index(['Date', 'SecCode'], inplace=True)

        closes = trade_data['closes']
        dividends = trade_data['dividends']
        splits = trade_data['splits']

        # Dates to iterate over - just one month plus one day
        unique_test_dates = np.unique(closes.keys())

        months = np.diff([x.month for x in unique_test_dates])

        # Get last day of period
        change_ind = np.where(months)[0][0]
        change_ind2 = change_ind + self._holding_period + 1
        unique_test_dates = unique_test_dates[:change_ind2]

        # Output object
        outdata_dates = []
        outdata_pl = []
        outdata_longpl = []
        outdata_shortpl = []
        outdata_turnover = []
        outdata_exposure = []
        outdata_openpositions = []

        sizes = SizeContainer(self._holding_period)

        for i, date in enumerate(unique_test_dates):

            portfolio.update_prices(
                closes[date], dividends[date], splits[date])

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()

            elif i <= change_ind:
                sizes.update_sizes(
                    i,
                    self.get_day_position_sizes(scores.loc[date],
                                                blank_allocs)
                )
                pos_sizes = sizes.get_sizes()
                portfolio.update_position_sizes(pos_sizes, closes[date])

            else:
                # Not putting on any new positions for this month's universe
                sizes.update_sizes(i)
                pos_sizes = sizes.get_sizes()
                portfolio.update_position_sizes(pos_sizes, closes[date])

            pl_long, pl_short = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            outdata_dates.append(date)
            outdata_pl.append((pl_long + pl_short) / BOOKSIZE)
            outdata_longpl.append(pl_long / BOOKSIZE)
            outdata_shortpl.append(pl_short / BOOKSIZE)
            outdata_turnover.append(daily_turnover / BOOKSIZE)
            outdata_exposure.append(daily_exposure)
            outdata_openpositions.append(sum([
                1 if x.shares != 0 else 0
                for x in portfolio.positions.values()]))

        daily_df = pd.DataFrame({
            'PL': outdata_pl,
            'LongPL': outdata_longpl,
            'ShortPL': outdata_shortpl,
            'Turnover': outdata_turnover,
            'Exposure': outdata_exposure,
            'OpenPositions': outdata_openpositions
        }, index=outdata_dates)

        return daily_df

    def get_day_position_sizes(self, scores, allocs):
        """
        For scores, Longs are low.
        For signals, Longs are high.
        """
        allocs = allocs.copy()

        counts = scores.shape[0] / 2

        longs = scores.iloc[counts:]
        shorts = scores.iloc[:counts]

        longs = longs.sort_values('scores')
        shorts = shorts.sort_values('scores', ascending=False)

        longs = longs.iloc[:self._per_side_count]
        shorts = shorts.iloc[:self._per_side_count]

        counts = self._per_side_count * 2
        for s in longs.index.values:
            allocs[s] = 1 / counts * BOOKSIZE
        for s in shorts.index.values:
            allocs[s] = -1 / counts * BOOKSIZE

        return allocs



class SizeContainer(object):

    def __init__(self, n_days):
        self.n_days = n_days
        self.sizes = {}

    def update_sizes(self, index, sizes={}):
        self.sizes[index] = sizes

    def get_sizes(self):
        # Init output with all seccods
        output = {x: 0 for x in set(sum([x.keys() for x
                                         in self.sizes.values()], []))}
        inds = self.sizes.keys()
        inds.sort()
        for i in inds[-self.n_days:]:
            for s, v in self.sizes[i].iteritems():
                output[s] += v / float(self.n_days)
        return output
