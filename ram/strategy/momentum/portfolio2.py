import numpy as np


class Portfolio2(object):

    def __init__(self, n_ports, n_positions):
        # Multiple portfolios because of different entry/exit dates
        self.portfolios = {(i+1): [] for i in range(n_ports)}
        # Multiply by two because n_positions is position per side
        self.div_factor = float(n_ports * n_positions * 2)
        self.closed_returns = []

    def add_positions(self, port_id, signals, prices):
        for symbol, signal in signals.iteritems():
            if signal != 0:
                self.portfolios[port_id].append(
                    Position(symbol, signal, prices[symbol]))

    def close_portfolio(self, port_id):
        closed_returns = []
        for pos in self.portfolios[port_id]:
            closed_returns.append(pos.get_return())
        self.portfolios[port_id] = []
        self.closed_returns = closed_returns

    def update_prices(self, prices):
        for port in self.portfolios.values():
            for pos in port:
                pos.update_price(prices[pos.symbol])

    def get_daily_return(self):
        returns = self.closed_returns
        self.closed_returns = []
        for port in self.portfolios.values():
            for pos in port:
                returns.append(pos.get_return())
        return sum(returns) / self.div_factor


class Position(object):

    def __init__(self, symbol, signal, price):
        self.symbol = symbol
        self.price1 = price
        self.price2 = price
        assert signal in [1, -1]
        self.signal = signal

    def update_price(self, price):
        self.price2 = price

    def get_return(self):
        p1 = float(self.price1)
        self.price1 = self.price2
        if np.isnan(p1) | np.isnan(self.price2):
            return 0
        return self.signal * (self.price2 / p1 - 1)
