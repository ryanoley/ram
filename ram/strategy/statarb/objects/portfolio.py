import numpy as np
import pandas as pd

from ram.strategy.statarb.objects.position import Position


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
                self.positions[symbol] = Position(symbol=symbol, price=close_)
            else:
                self.positions[symbol].update_position_prices(
                    close_, dividends[symbol], splits[symbol])
        return

    def update_position_sizes(self, sizes, exec_prices):
        portfolio_symbols = self.positions.keys()
        new_symbols = set(sizes.keys()) - set(portfolio_symbols)
        # Update positions in portfolio
        for symbol in portfolio_symbols:
            if symbol in sizes:
                self.positions[symbol].update_position_size(
                    sizes[symbol], exec_prices[symbol])
            # Remove symbols that have no pricing provided
            elif symbol not in exec_prices:
                if self.positions[symbol].shares == 0:
                    del self.positions[symbol]
                else:
                    raise ValueError('Still open shares')
            # Otherwise just set position to size 0
            else:
                self.positions[symbol].update_position_size(
                    0, exec_prices[symbol])
        # Add new symbols
        for symbol in new_symbols:
            self.positions[symbol] = Position(symbol=symbol,
                                              price=exec_prices[symbol])
            self.positions[symbol].update_position_size(
                    sizes[symbol], exec_prices[symbol])
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
        output = {'worst_losing_day_count': 0}
        for position in self.positions.values():
            stats = position.get_stats()
            output['worst_losing_day_count'] = \
                max(output['worst_losing_day_count'],
                    stats['losing_day_count'])
        return output

    def close_portfolio_positions(self):
        for position in self.positions.values():
            position.close_position()
