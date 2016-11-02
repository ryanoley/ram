import pandas as pd

from ram.strategy.base import Strategy
from ram.data.dh_sql import DataHandlerSQL


class BenchmarksStrategy(Strategy):

    def __init__(self):
        self.data = DataHandlerSQL()

    def get_results(self):
        return self.results

    def start(self):
        prices = self.data.get_id_data(
            ids='SPY',
            features=['ADJ_Close'],
            start_date='1993-01-30',
            end_date='2020-01-01')
        # Daily returns for the SPY
        prices = prices.set_index('Date')
        prices['SPY'] = prices.ADJ_Close.pct_change()
        prices = prices.drop(['ID', 'ADJ_Close'], axis=1).dropna()
        self.results = prices

    def start_live(self):
        return -9999


if __name__ == '__main__':
    strategy = BenchmarksStrategy()
    strategy.start()
    # strategy.start_live()
    # strategy.get_results()
