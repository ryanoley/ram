from pandas.io.data import DataReader
from gearbox import convert_date_array

from ram.strategy.base import Strategy


class Benchmarks(Strategy):

    def __init__(self):
        pass

    def get_results(self):
        return self.results

    def start(self):
        self.results = self._get_data()

    def start_live(self):
        # Get all data
        results = self._get_data()
        self.results = results.iloc[-1:]

    def _get_data(self):
        # TEMP!!!
        prices = DataReader('SPY', 'yahoo')
        prices.index = convert_date_array(prices.index.astype(str))
        prices = prices[['Adj Close']]
        prices.columns = ['SPY']
        return prices.pct_change().dropna()


if __name__ == '__main__':

    strategy = Benchmarks()
    strategy.start()
    # strategy.start_live()
    print strategy.get_results()
