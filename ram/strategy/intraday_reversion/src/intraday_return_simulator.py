
from ram.strategy.intraday_reversion.src.import_data import *

from ram.strategy.intraday_reversion.src.take_stop_returns import *


class IntradayReturnSimulator(object):

    def __init__(self):
        self.tickers = get_available_tickers()
        self._bar_data = {}

    def get_returns(self, signals):
        """
        Signals has: Ticker, Date, signals
        """
        assert 'Ticker' in signals
        assert 'Date' in signals
        assert 'signal' in signals

        for ticker in signals.Ticker.unique():
            rets = self._retrieve_return_data(ticker)


    def _retrieve_return_data(self, ticker):
        if ticker not in self._bar_data:
            self._bar_data[ticker] = get_intraday_rets_data(ticker)
        return self._bar_data[ticker]
