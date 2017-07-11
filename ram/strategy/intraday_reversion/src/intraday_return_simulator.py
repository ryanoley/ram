from ram.strategy.intraday_reversion.src.import_data import *
from ram.strategy.intraday_reversion.src.take_stop_returns import *


class IntradayReturnSimulator(object):

    def __init__(self):
        self.tickers = get_available_tickers()
        self._bar_data = {}
        self._response_data = {}

    # ~~~~~~ Returns for equity curve simulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_returns(self, signals, perc_take, perc_stop):
        """
        Signals has: Ticker, Date, signals
        """
        assert 'Ticker' in signals
        assert 'Date' in signals
        assert 'signal' in signals
        assert perc_take >= 0
        assert perc_stop >= 0

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

    # ~~~~~~ Responses for learning model ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_responses(self, ticker, perc_take, perc_stop):
        assert perc_take >= 0
        assert perc_stop >= 0
        # To assert that it can't be a long and short winner
        assert perc_stop <= perc_take

        # See if data has already been processed
        response_label = '{}_{}'.format(perc_take, perc_stop)
        if ticker in self._response_data:
            if response_label in self._response_data[ticker]:
                return self._response_data[ticker][response_label]

        # Calculate responses
        ret_data = self._retrieve_return_data(ticker)
        long_rets = get_long_returns(ret_data[0], ret_data[1], ret_data[2],
                                     perc_take, perc_stop)
        short_rets = get_short_returns(ret_data[0], ret_data[1],
                                       ret_data[2], perc_take, perc_stop)
        # Create output for daily responses
        output = pd.DataFrame(index=ret_data[0].columns)
        output['Ticker'] = ticker

        # IS THIS HOW WE WANT TO CODE RESPONSES??
        output['response'] = np.where(
            long_rets == perc_take, 1, np.where(
            short_rets == perc_take, -1, 0))

        # Cache processed data
        if not ticker in self._response_data:
            self._response_data[ticker] = {}
        self._response_data[ticker][response_label] = output

        return output

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
