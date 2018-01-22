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
            'score_var': ['ret4~prma_5~1',
                          'ret4~prma_5~2',
                          'ret4~prma_5~3',
                          'ret4~prma_5~4',
                          'ret4~prma_5~5',
                          'ret4~prma_5~6',

                          'ret2~prma_5~1',
                          'ret2~prma_5~2',
                          'ret2~prma_5~3',
                          'ret2~prma_5~4',
                          'ret2~prma_5~5',
                          'ret2~prma_5~6',

                          'prma_5~ret2~1',
                          'prma_5~ret2~2',
                          'prma_5~ret2~3',
                          'prma_5~ret2~4',
                          'prma_5~ret2~5',
                          'prma_5~ret2~6',

                          'prma_3_10~ret2~1',
                          'prma_3_10~ret2~2',
                          'prma_3_10~ret2~3',
                          'prma_3_10~ret2~4',
                          'prma_3_10~ret2~5',
                          'prma_3_10~ret2~6',

                          'prma_3_10~day_ret~1',
                          'prma_3_10~day_ret~2',
                          'prma_3_10~day_ret~3',
                          'prma_3_10~day_ret~4',
                          'prma_3_10~day_ret~5',
                          'prma_3_10~day_ret~6'

                          ],

            'holding_period': [5, 2],
            'flip_var': [True, False]
        })

    def set_args(self, score_var, holding_period, flip_var):
        self._score_var = score_var
        self._holding_period = holding_period
        self._flip_var = flip_var

    def process(self, trade_data, signals):

        var1, var2, region = self._score_var.split('~')

        portfolio = Portfolio()

        features = trade_data['score_data']

        closes = trade_data['closes']
        dividends = trade_data['dividends']
        splits = trade_data['splits']
        liquidity = trade_data['liquidity']

        # Dates to iterate over - just one month plus one day
        unique_test_dates = np.unique(closes.keys())

        months = np.diff([x.month for x in unique_test_dates])

        change_ind = np.where(months)[0][0] + 1

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
                    self.get_day_position_sizes(features[date],
                                                var1, var2, region)
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

    def get_day_position_sizes(self, features, var1, var2, region):

        allocs = {x: 0 for x in features.SecCode}

        features = features.loc[features.keep_inds,
                                ['SecCode', var1, var2]].copy()
        features = features.sort_values(var1)

        count = features.shape[0] // 2
        features_1 = features.iloc[:count].copy()
        features_2 = features.iloc[count:].copy()
        features_1 = features_1.sort_values(var2)
        features_2 = features_2.sort_values(var2)
        count = features_1.shape[0] // 2

        if region == '1':
            side_1 = features_1.iloc[:count].copy()
            side_2 = features_1.iloc[count:].copy()

        elif region == '2':
            side_1 = features_1.iloc[:count].copy()
            side_2 = features_2.iloc[:count].copy()

        elif region == '3':
            side_1 = features_1.iloc[:count].copy()
            side_2 = features_2.iloc[count:].copy()

        elif region == '4':
            side_1 = features_1.iloc[count:].copy()
            side_2 = features_2.iloc[:count].copy()

        elif region == '5':
            side_1 = features_1.iloc[count:].copy()
            side_2 = features_2.iloc[count:].copy()

        else:
            side_1 = features_2.iloc[:count].copy()
            side_2 = features_2.iloc[count:].copy()

        if self._flip_var:
            side_1['side'] = 1
            side_2['side'] = -1

        else:
            side_1['side'] = -1
            side_2['side'] = 1

        side = side_1.append(side_2)

        for i, x in side.iterrows():
            allocs[x['SecCode']] = x['side']

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
