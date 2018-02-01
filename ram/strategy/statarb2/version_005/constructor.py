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
             'score_var': ['a']
        })

    def set_args(self, score_var):
        self._score_var = score_var

    def process(self, trade_data, signals):

        portfolio = Portfolio()
        self.port = PortfolioContainer()

        zscores = trade_data['pair_data']['zscores']

        closes = trade_data['closes']
        dividends = trade_data['dividends']
        splits = trade_data['splits']
        liquidity = trade_data['liquidity']

        # Dates to iterate over - just one month plus one day
        unique_test_dates = trade_data['test_dates'].values

        months = np.diff([x.month for x in unique_test_dates])

        change_ind = np.where(months)[0][0] + 2

        unique_test_dates = unique_test_dates[:change_ind]

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
                pos_sizes = self.get_day_position_sizes(zscores.loc[date])

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

    def get_day_position_sizes(self, zscores):
        # Check if any portfolios need to be closed
        for i in range(len(zscores)):
            self.port.check_zscore(zscores.index[i], zscores[i])
        new_ports = self.port.num_ports - len(self.port._ports)
        # Sort by abs value and start adding to fill up day's limit
        zscores = zscores[np.argsort(-1*np.abs(zscores)).values]
        i = 0
        for i in range(len(zscores)):
            if i == new_ports:
                break
            if np.abs(zscores[i]) < 1.6:
                break
            if self.port.check_new_port(zscores.index[i],
                                        -np.sign(zscores[i])):
                self.port.add_port(zscores.index[i], -np.sign(zscores[i]))
                i += 1
        allocs = self.port.get_sizes()
        allocs = {s: v * BOOKSIZE for s, v in allocs.iteritems()}
        return allocs


class PortfolioContainer(object):

    def __init__(self, num_ports=20, pos_max_exposure=0.05, exit_z=1):
        self.num_ports = num_ports
        self.pos_max_exposure = pos_max_exposure
        self.exit_z = exit_z
        self._ports = {}
        self._positions = {}

    def get_sizes(self):
        norm_factor = len(self._ports) * 4.
        positions = self._positions.copy()
        return {x: y / norm_factor for x, y in positions.iteritems()}

    def check_zscore(self, port_id, zscore):
        if port_id not in self._ports:
            return
        x1, x2 = port_id.split('~')
        x11, x12 = x1.split('_')
        x21, x22 = x2.split('_')
        side = self._ports[port_id]
        if side == 1:
            if zscore > -self.exit_z:
                del self._ports[port_id]
                self._positions[x11] += -1
                self._positions[x12] += -1
                self._positions[x21] += 1
                self._positions[x22] += 1
        else:
            if zscore < self.exit_z:
                del self._ports[port_id]
                self._positions[x11] += 1
                self._positions[x12] += 1
                self._positions[x21] += -1
                self._positions[x22] += -1

    def check_new_port(self, port_id, side):
        if port_id in self._ports:
            return False
        # Check positions
        x1, x2 = port_id.split('~')
        x11, x12 = x1.split('_')
        x21, x22 = x2.split('_')
        if not self._check_position(x11, side):
            return False
        if not self._check_position(x12, side):
            return False
        if not self._check_position(x21, -side):
            return False
        if not self._check_position(x22, -side):
            return False
        return True

    def add_port(self, port_id, side):
        """
        Side corresponds to the first portfolio of port_id
        """
        assert port_id not in self._ports
        x1, x2 = port_id.split('~')
        x11, x12 = x1.split('_')
        x21, x22 = x2.split('_')
        self._ports[port_id] = side
        self._update_position(x11, side)
        self._update_position(x12, side)
        self._update_position(x21, -side)
        self._update_position(x22, -side)

    def _update_position(self, pos_id, side):
        if pos_id not in self._positions:
            self._positions[pos_id] = 0
        self._positions[pos_id] += side

    def _check_position(self, pos_id, side):
        if pos_id not in self._positions:
            return True
        count = self._positions[pos_id] + side
        if abs(count) > int(self.num_ports * 4 * self.pos_max_exposure):
            return False
        else:
            return True
