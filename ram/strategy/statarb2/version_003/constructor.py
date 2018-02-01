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
            'some_var': [1]
        })

    def set_args(self, some_var):
        self._some_var = some_var

    def process(self, trade_data, signals):

        portfolio = Portfolio()

        closes = trade_data['closes']
        dividends = trade_data['dividends']
        splits = trade_data['splits']
        liquidity = trade_data['liquidity']

        # Dates to iterate over - just one month plus one day
        unique_test_dates = np.unique(closes.keys())

        months = np.diff([x.month for x in unique_test_dates])

        change_ind = np.where(months)[0][0] + 1

        # Output object
        outdata_dates = []
        outdata_pl = []
        outdata_longpl = []
        outdata_shortpl = []
        outdata_turnover = []
        outdata_exposure = []
        outdata_openpositions = []

        for i, date in enumerate(unique_test_dates):

            portfolio.update_prices(
                closes[date], dividends[date], splits[date])

            if date == unique_test_dates[change_ind]:
                portfolio.close_portfolio_positions()

            elif date > unique_test_dates[change_ind]:
                continue

            else:
                pos_sizes = self.get_day_position_sizes(signals.loc[date])
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

    def get_day_position_sizes(self, scores):
        allocs = {x: 0 for x in scores.index}
        scores2 = scores.rank(pct=True)
        scores2 = (scores2 - scores2.median()).fillna(0)
        return (BOOKSIZE * scores2 / scores2.abs().sum()).to_dict()


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
