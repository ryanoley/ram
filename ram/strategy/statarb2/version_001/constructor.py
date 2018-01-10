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
            'prma_x': [5, 10, 15, 20],
            'split_perc': [20, 30, 40],
            'daily_drop': [True, False],
            'holding_period': [10, 5, 2],
            'month_end_close': [True, False]
        })

    def set_args(self,
                 prma_x,
                 split_perc,
                 daily_drop,
                 holding_period,
                 month_end_close):
        self._prma_x = prma_x
        self._split_perc = split_perc
        self._daily_drop = daily_drop
        self._holding_period = holding_period
        self._month_end_close = month_end_close

    def process(self, trade_data):

        portfolio = Portfolio()

        scores = trade_data['prma_{}'.format(self._prma_x)]
        day_ret = trade_data['day_ret_rank']
        day_ret_abs = trade_data['day_ret_abs']

        closes = trade_data['closes']
        dividends = trade_data['dividends']
        splits = trade_data['splits']
        liquidity = trade_data['liquidity']

        # Dates to iterate over - just one month plus one day
        unique_test_dates = np.unique(closes.keys())

        months = np.diff([x.month for x in unique_test_dates])

        change_ind = np.where(months)[0][0] + 1

        if self._month_end_close:
            unique_test_dates = unique_test_dates[:(change_ind+1)]

        else:
            unique_test_dates = unique_test_dates[:change_ind+self._holding_period]

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

            elif i < change_ind:
                sizes.update_sizes(
                    i,
                    self.get_day_position_sizes(scores.loc[date],
                                                day_ret.loc[date],
                                                day_ret_abs.loc[date])
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

    def get_day_position_sizes(self, scores, day_ret, day_ret_abs):

        allocs = {x: 0 for x in scores.index}

        if self._daily_drop:
            longs = scores[day_ret < 0.5]
            shorts = scores[day_ret >= 0.5]
            longs = longs.sort_values()
            shorts = shorts.sort_values(ascending=False)

        else:
            scores2 = scores.sort_values().dropna()
            split = len(scores2)/2
            longs = scores2[:split]
            shorts = scores2[split:]
            shorts = shorts.sort_values(ascending=False)

        counts = int(len(longs) * 2 * (self._split_perc * 0.01))
        longs = longs.iloc[:counts]
        shorts = shorts.iloc[:counts]

        for s in longs.index:
            allocs[s] = 1
        for s in shorts.index:
            allocs[s] = -1

        counts = float(sum([abs(x) for x in allocs.values()]))
        allocs = {s: v / counts * BOOKSIZE for s, v in allocs.iteritems()}

        return allocs


def filter_seccodes(data_dict, min_value):
    bad_seccodes = []
    for key, value in data_dict.iteritems():
        if value < min_value:
            bad_seccodes.append(key)
        elif np.isnan(value):
            bad_seccodes.append(key)
    return bad_seccodes


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
