import numpy as np
import pandas as pd

from ram.strategy.starmine.constructor.position import Position
from ram.strategy.starmine.constructor.hedged_position import HedgedPosition


class Portfolio(object):

    def __init__(self):
        self.positions = {}

    def update_prices(self, closes, dividends, splits):
        """
        Should be first step in daily loop!

        Parameters
        ----------
        closes/dividends/splits : dict
        """
        for symbol, close_ in closes.iteritems():
            if symbol not in self.positions:
                self.positions[symbol] = HedgedPosition(symbol=symbol, price=close_)
            else:
                self.positions[symbol].update_position_prices(
                    close_, dividends[symbol], splits[symbol])
        return

    def update_position_sizes(self, sizes, exec_prices):
        for symbol, size in sizes.iteritems():
            self.positions[symbol].update_position_size(
                size, exec_prices[symbol])
        return

    def get_portfolio_exposure(self):
        return sum([abs(pos.exposure) for pos in self.positions.itervalues()])

    def get_portfolio_daily_pl(self):
        port_daily_pl_long = 0
        port_daily_pl_short = 0
        for position in self.positions.values():
            if position.shares >= 0:
                port_daily_pl_long += position.get_daily_pl()
            else:
                port_daily_pl_short += position.get_daily_pl()
        return port_daily_pl_long, port_daily_pl_short

    def get_portfolio_daily_turnover(self):
        port_turnover = 0
        for position in self.positions.values():
            port_turnover += position.get_daily_turnover()
        return port_turnover

    def get_portfolio_stats(self):
        sector_pl = {}
        sector_counts = {}

        for position in self.positions.values():
            sector = str(position.sector)
            if sector not in sector_pl.keys():
                sector_pl[sector] = position.daily_pl
                sector_counts[sector] = 1
            else:
                sector_pl[sector] += position.daily_pl
                sector_counts[sector] += 1

        sum_df = pd.DataFrame(data={'counts':sector_counts,
                                    'pl':sector_pl},
                              index = sector_counts.keys())
        sum_df['AvgPL'] = sum_df.pl / sum_df.counts
        sum_df = sum_df.T
        sum_df.columns = ['sector_{}_pl'.format(x) for x in sum_df.columns]
        return sum_df.loc['AvgPL'].to_dict()

    def close_portfolio_positions(self):
        for position in self.positions.values():
            position.close_position()

    def dd_filter(self, drawdown_pct=-.05, dd_from_zero=False):
        dd_seccodes = set()

        for position in self.positions.values():
            if position.exposure != 0:
                if dd_from_zero:
                    drawdown = position.cumulative_return
                else:
                    drawdown = (position.cumulative_return - position.return_peak)

                if drawdown <= drawdown_pct:
                    dd_seccodes.add(position.symbol)

        return dd_seccodes

    def get_open_positions(self):
        open_longs = set()
        open_shorts = set()
        for position in self.positions.values():
            if position.exposure > 0:
                open_longs.add(position.symbol)
            elif position.exposure < 0:
                open_shorts.add(position.symbol)
        return open_longs, open_shorts

    def update_mkt_prices(self, mkt_price):
        for position in self.positions.values():
            if position.exposure != 0:
                position.update_mkt_prices(mkt_price)
        return

    def add_sector_info(self, sectors):
        for position in self.positions.values():
            if position.symbol in sectors.keys():
                sector = sectors[position.symbol]
                if len(sector) != 1:
                    continue
                elif not np.isnan(sector[0]):
                    sector = str(sector[0])[:2]
                    position.set_sector(sector)
        return


    def reset_daily_pl(self):
        for position in self.positions.values():
            position.reset_daily_pl()
        return