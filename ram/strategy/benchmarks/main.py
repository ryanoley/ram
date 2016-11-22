from ram.strategy.base import Strategy


class BenchmarksStrategy(Strategy):

    def get_iter_index(self):
        return [0]

    def run_index(self, index):
        prices = self.datahandler.get_etf_data(
            tickers='SPY',
            features=['Close'],
            start_date='1993-01-30',
            end_date='2020-01-01')
        # Daily returns for the SPY
        prices = prices.set_index('Date')
        prices['SPY'] = prices.Close.pct_change()
        prices = prices.drop(['SecCode', 'Close'], axis=1).dropna()
        return prices


if __name__ == '__main__':

    strategy = BenchmarksStrategy()
    strategy.start()
