import numpy as np
import pandas as pd

from ram.strategy.starmine.constructor.position import Position
from ram.strategy.starmine.constructor.hedged_position import HedgedPosition


class Portfolio(object):

    def __init__(self):
        self.positions = {}

    def update_prices(self, closes):
        """
        Should be first step in daily loop!

        Parameters
        ----------
        closes/dividends/splits : dict
        """
        for symbol, close_ in closes.iteritems():
            if symbol not in self.positions:
                self.positions[symbol] = HedgedPosition(symbol=symbol,
                                                        price=close_)
            else:
                self.positions[symbol].update_position_prices(close_)
        return

    def update_splits_dividends(self, splits, dividends):
        for symbol, dividend in dividends.iteritems():
            if dividend != 0.:
                self.positions[symbol].dividend_adjustment(dividend)

        for symbol, split in splits.iteritems():
            if split != 1.:
                self.positions[symbol].split_adjustment(split)

        return

    def update_position_sizes(self, sizes, exec_prices):
        for symbol, size in sizes.iteritems():
            self.positions[symbol].update_position_size(
                size, exec_prices[symbol])
        return

    def get_portfolio_position_totals(self):
        n_longs = 0
        n_shorts = 0
        for position in self.positions.itervalues():
            if position.symbol == 'HEDGE':
                continue
            elif position.shares > 0:
                n_longs += 1
            elif position.shares < 0:
                n_shorts += 1
        return n_longs, n_shorts

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

    def dd_filter(self, drawdown_pct=-.05):
        dd_seccodes = set()
        if np.abs(drawdown_pct) > 1:
            return dd_seccodes

        for position in self.positions.values():
            if ((position.exposure != 0) &
                (position.cumulative_return <= drawdown_pct)):
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

    def get_position_weights(self):
        weights = pd.Series(name='weight', index=self.positions.keys())
        for position in self.positions.values():
            weights.loc[position.symbol] = position.weight
        spy_mask = ~weights.index.isin(['HEDGE'])
        return weights[spy_mask]

    def update_position_weights(self, weights):
        for symbol, weight in weights.items():
            self.positions[symbol].set_weight(weight)
        return

    def update_holding_days(self, hold_days):
        for symbol, weight in hold_days.items():
            if weight != 0:
                self.positions[symbol].hold_days += 1
        return

    def update_mkt_prices(self, mkt_price):
        for position in self.positions.values():
            if (position.exposure != 0) & (position.symbol != 'HEDGE'):
                position.update_mkt_price(mkt_price)
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
