import numpy as np
import pandas as pd

from ram.strategy.analyst_estimates.base.hedged_position import HedgedPosition


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

    def update_position_sizes(self, sizes, exec_prices):
        for symbol, size in sizes.iteritems():
            self.positions[symbol].update_position_size(
                size, exec_prices[symbol])
        return

    def add_sector_info(self, sectors):
        for symbol, sector in sectors.iteritems():
            if symbol in self.positions.keys():
                position = self.positions[symbol]
                if len(sector) != 1:
                    continue
                elif not np.isnan(sector[0]):
                    sector = str(sector[0])[:2]
                    position.set_sector(sector)
        return

    def update_splits_dividends(self, splits, dividends):
        for symbol, dividend in dividends.iteritems():
            if dividend != 0.:
                self.positions[symbol].dividend_adjustment(dividend)

        for symbol, split in splits.iteritems():
            if split != 1.:
                self.positions[symbol].split_adjustment(split)

        return

    def get_portfolio_daily_pl(self):
        port_daily_pl_long = 0.
        port_daily_pl_short = 0.
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
            position.reset_daily_turnover()
        return port_turnover

    def get_portfolio_exposure(self):
        daily_exposure = 0
        for pos in self.positions.itervalues():
            daily_exposure += abs(pos.exposure)
        return daily_exposure

    def get_position_weights(self):
        weights = {}
        for position in self.positions.values():
            weights[position.symbol] = position.weight
        weights = pd.Series(data=weights, name='weight')
        spy_mask = ~weights.index.isin(['HEDGE'])
        return weights[spy_mask]

    def get_daily_df_data(self):
        port_daily_pl_long = 0.
        port_daily_pl_short = 0.
        port_turnover = 0
        daily_exposure = 0
        weights = {}

        for position in self.positions.values():
            if position.shares >= 0:
                port_daily_pl_long += position.get_daily_pl()
            else:
                port_daily_pl_short += position.get_daily_pl()
            port_turnover += position.get_daily_turnover()
            daily_exposure += abs(position.exposure)
            weights[position.symbol] = position.weight

        weights = pd.Series(data=weights, name='weight')
        spy_mask = ~weights.index.isin(['HEDGE'])

        return port_daily_pl_long, port_daily_pl_short, port_turnover, \
                    daily_exposure, weights[spy_mask]

    def dd_filter(self, drawdown_pct=-.05):
        dd_seccodes = set()
        if np.abs(drawdown_pct) > 1:
            return dd_seccodes

        for position in self.positions.values():
            if ((position.exposure != 0) &
                (position.cumulative_return <= drawdown_pct)):
                    dd_seccodes.add(position.symbol)

        return dd_seccodes

    def get_portfolio_stats(self):
        sector_counts = {}

        for position in self.positions.values():
            sector = 'sector_{}_n'.format(position.sector)
            if sector not in sector_counts.keys():
                sector_counts[sector] = 0
            if position.daily_pl != 0.:
                sector_counts[sector] += 1
        return sector_counts

    def close_portfolio_positions(self):
        for position in self.positions.values():
            position.close_position()

    def update_position_weights(self, weights):
        for symbol, weight in weights.items():
            self.positions[symbol].set_weight(weight)
        return

    def update_holding_days(self, weight_dict):
        for symbol, weight in weight_dict.items():
            if weight != 0:
                self.positions[symbol].hold_days += 1
        return

    def update_hedge_prices(self, mkt_price):
        for position in self.positions.values():
            if (position.exposure != 0) & (position.symbol != 'HEDGE'):
                position.update_hedge_price(mkt_price)
        return

    def reset_daily_pl_exposure(self):
        for position in self.positions.values():
            position.reset_daily_pl()
            position.reset_daily_turnover()
        return
