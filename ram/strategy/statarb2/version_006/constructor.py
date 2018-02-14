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
            'score_var': ['prma_5', 'prma_10', 'prma_15', 'prma_20'],
            'split_perc': [20, 30, 40],
            #'n_ports': [1, 2, 3, 4, 5]
            'n_ports': [1]
        })

    def set_args(self,
                 score_var,
                 split_perc,
                 n_ports):
        self._score_var = score_var
        self._split_perc = split_perc
        self._n_ports = n_ports

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
        change_ind = np.where(months)[0][0] + 1
        unique_test_dates = unique_test_dates[:(change_ind+1)]

        # Get rebalance indexes
        # rebalance_inds = np.linspace(
        #     0, len(unique_test_dates), self._n_ports+1).astype(int)
        # rebalance_inds = rebalance_inds[:-1]

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


            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()

            else:
            #elif i in rebalance_inds:
                sizes = self.get_day_position_sizes(scores.loc[date],
                                                    signals.loc[date])
                portfolio.update_position_sizes(sizes, closes[date])

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

        df = pd.DataFrame({'signals': signals, 'scores': scores}).dropna()
        df = df.sort_values('signals')

        counts = df.shape[0] / 2

        longs = df.iloc[counts:]
        shorts = df.iloc[:counts]

        longs = longs.sort_values('scores')
        shorts = shorts.sort_values('scores', ascending=False)

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
