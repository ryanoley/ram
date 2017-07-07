from ram.strategy.intraday_reversion.src.import_data import *
from ram.strategy.intraday_reversion.src.take_stop_returns import *


class IntradayReturnSimulator(object):

    def __init__(self):
        self.tickers = get_available_tickers()
        self._bar_data = {}

    def get_returns(self, signals, perc_take, perc_stop):
        """
        Signals has: Ticker, Date, signals
        """
        assert 'Ticker' in signals
        assert 'Date' in signals
        assert 'signal' in signals

        tickers = signals.Ticker.unique()
        ticker_returns = pd.DataFrame()
        for ticker in tickers:
            # Segregate ticker signals
            sigs = signals[signals.Ticker == ticker]
            # Get intraday data
            ret_data = self._retrieve_return_data(ticker)
            # Calculate all intraday returns
            long_rets = get_long_returns(ret_data[0], ret_data[1], ret_data[2],
                                         perc_take, perc_stop)
            short_rets = get_short_returns(ret_data[0], ret_data[1],
                                           ret_data[2], perc_take, perc_stop)
            ticker_returns = ticker_returns.join(
                self._get_returns_from_signals(sigs, long_rets, short_rets),
                how='outer')
        return ticker_returns.sum(axis=1) / \
            (~ticker_returns.isnull()).sum(axis=1)

    def _get_returns_from_signals(self, signals, longs, shorts):
        longs = longs.reset_index()
        longs.columns = ['Date', 'LongRet']
        shorts = shorts.reset_index()
        shorts.columns = ['Date', 'ShortRet']
        signals2 = signals.merge(longs, how='left').merge(shorts, how='left')
        signals2['rets'] = np.where(
            signals2.signal == 1, signals2.LongRet, np.where(
            signals2.signal == -1, signals2.ShortRet, 0))
        signals2 = signals2[['Date', 'rets']].fillna(0).set_index('Date')['rets']
        signals2.name = signals.Ticker.iloc[0]
        return signals2

    def _retrieve_return_data(self, ticker):
        if ticker not in self._bar_data:
            self._bar_data[ticker] = get_intraday_rets_data(ticker)
        return self._bar_data[ticker]
