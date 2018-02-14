import numba
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.utils import make_arg_iter
from ram.strategy.statarb.utils import make_variable_dict
from ram.strategy.statarb2.portfolio import Portfolio

BOOKSIZE = 2e6


class PortfolioConstructor(object):

    def get_args(self):
        return make_arg_iter({
            'score_var': ['prma_5', 'prma_10', 'prma_15',
                          'prma_20', 'ret_10d'],
            'split_perc': [20, 30, 40],
            'holding_period': [3, 4, 5],
            'sort_signal_first': [False, True]
        })

    def set_args(self,
                 score_var,
                 split_perc,
                 holding_period,
                 sort_signal_first):
        self._score_var = score_var
        self._split_perc = split_perc
        self._holding_period = holding_period
        self._sort_signal_first = sort_signal_first

    def process(self, trade_data, signals):

        portfolio = Portfolio()

        scores = trade_data[self._score_var]

        closes = trade_data['closes']
        dividends = trade_data['dividends']
        splits = trade_data['splits']
        liquidity = trade_data['liquidity']

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
                                                signals.loc[date])
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

    def get_day_position_sizes(self, scores, signals):

        allocs = {x: 0 for x in scores.index}

        if self._sort_signal_first:
            df = pd.DataFrame({'first_sort': signals,
                               'second_sort': scores}).dropna()
        else:
            # NOTE: Flip of scores and signals
            df = pd.DataFrame({'first_sort': -1 * scores,
                               'second_sort': -1 * signals}).dropna()

        df = df.sort_values('first_sort')
        counts = df.shape[0] / 2

        longs = df.iloc[counts:]
        shorts = df.iloc[:counts]

        longs = longs.sort_values('second_sort')
        shorts = shorts.sort_values('second_sort', ascending=False)

        counts = int(longs.shape[0] * (self._split_perc * 0.01))
        longs = longs.iloc[:counts]
        shorts = shorts.iloc[:counts]

        for s in longs.index:
            allocs[s] = 1
        for s in shorts.index:
            allocs[s] = -1

        counts = float(sum([abs(x) for x in allocs.values()]))
        allocs = {s: v / counts * BOOKSIZE for s, v in allocs.iteritems()}

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
