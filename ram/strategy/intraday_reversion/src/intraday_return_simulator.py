import itertools
import pandas as pd
from tqdm import tqdm

from ram.strategy.intraday_reversion.src.import_data import *
from ram.strategy.intraday_reversion.src.take_stop_returns import *

# !! NOTE: All transaction costs are hard-coded into take_stop_returns.py


class IntradayReturnSimulator(object):

    def __init__(self):
        self.tickers = get_available_tickers()
        self._hlc_rets_data = {}
        self._response_data = {}
        self._return_data = {}

    # ~~~~~~ Returns for equity curve simulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_returns(self, signals):
        """
        Signals has: Ticker, Date, signals
        """
        assert 'Ticker' in signals
        assert 'Date' in signals
        assert 'signal' in signals
        assert 'perc_take' in signals
        assert 'perc_stop' in signals
        assert np.all(signals.perc_take >= 0)
        assert np.all(signals.perc_stop >= 0)

        tickers = signals.Ticker.unique()
        ticker_returns = pd.DataFrame()

        for ticker in tickers:
            ticker_returns = ticker_returns.append(
                signals[signals.Ticker == ticker].merge(
                    self._return_data[ticker], how='left'))

        ticker_returns.Return = ticker_returns.Return.fillna(0)
        ticker_returns = ticker_returns.pivot(
            index='Date', columns='Ticker', values='Return')
        # STATISTICS
        stats = {}
        stats.update(self._get_ticker_stats(ticker_returns))
        return ticker_returns.sum(axis=1) / \
            (~ticker_returns.isnull()).sum(axis=1), stats

    # ~~~~~~ Preprocess returns given params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def preprocess_returns(self, perc_take, perc_stop, tickers=None):

        if not isinstance(perc_take, list):
            perc_take = [perc_take]
        if not isinstance(perc_stop, list):
            perc_stop = [perc_stop]
        if tickers is None:
            tickers = self.tickers
        if not isinstance(tickers, list):
            tickers = [tickers]

        print('Preprocessing Take/Stop returns:')
        for ticker, take, stop in tqdm(list(itertools.product(tickers,
                                                              perc_take,
                                                              perc_stop))):
            self._preprocess_returns(ticker, take, stop)

    def _preprocess_returns(self, ticker, perc_take, perc_stop):
        # See if any data has been calculated for this ticker
        if ticker not in self._return_data:
            self._return_data[ticker] = pd.DataFrame()

        long_rets, short_rets = self._get_long_short_rets(
            ticker, perc_take, perc_stop)

        # Stack for get_returns calculation
        stacked = pd.DataFrame({'Return': long_rets, 'signal': 1}).append(
            pd.DataFrame({'Return': short_rets, 'signal': -1}))
        stacked = stacked.reset_index()
        stacked['perc_take'] = perc_take
        stacked['perc_stop'] = perc_stop
        stacked['Ticker'] = ticker
        stacked = stacked[['Date', 'Ticker', 'Return',
                           'perc_take', 'perc_stop', 'signal']]
        self._return_data[ticker] = self._return_data[ticker].append(stacked)

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
        long_rets, short_rets = self._get_long_short_rets(
            ticker, perc_take, perc_stop, False)

        # Create output for daily responses
        output = pd.DataFrame(index=long_rets.index)
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
        """
        Creates a cache of open/high/low return data
        """
        if ticker not in self._hlc_rets_data:
            self._hlc_rets_data[ticker] = get_intraday_rets_data(ticker)
        return self._hlc_rets_data[ticker]

    def _get_long_short_rets(self, ticker, perc_take, perc_stop,
                             costs_flag=True):
        ret_data = self._retrieve_return_data(ticker)
        if costs_flag:
            long_rets = get_long_returns(ret_data[0], ret_data[1], ret_data[2],
                                         ret_data[3], ret_data[4],
                                         perc_take, perc_stop)
            short_rets = get_short_returns(ret_data[0], ret_data[1],
                                           ret_data[2], ret_data[3],
                                           ret_data[4], perc_take, perc_stop)
        else:
            # For response calculation with no costs taken out
            long_rets = get_long_returns(ret_data[0], ret_data[1], ret_data[2],
                                         ret_data[3] * 0, ret_data[4] * 0,
                                         perc_take, perc_stop)
            short_rets = get_short_returns(ret_data[0], ret_data[1],
                                           ret_data[2], ret_data[3] * 0,
                                           ret_data[4] * 0,
                                           perc_take, perc_stop)
        return long_rets, short_rets

    # ~~~~~~ Stats ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_ticker_stats(self, returns):
        returns = returns.copy()
        # Fill nan until first non-zero return so to not count
        # training data
        for col in returns.columns:
            ind = np.where(returns[col] != 0)[0][0]
            if ind > 0:
                returns.loc[:ind, col] = np.nan
        trades = ((returns != 0) & (~returns.isnull())).sum()
        winners = (returns > 0).sum()
        counts = (~returns.isnull()).sum()
        total_returns = returns.sum()
        #
        participation = trades / counts.astype(float)
        win_percent = winners / trades.astype(float)
        # Convert names
        participation = {'participation_{}'.format(key): val for key, val in  participation.iteritems()}
        win_percent = {'win_percent_{}'.format(key): val for key, val in  win_percent.iteritems()}
        total_returns = {'total_return_{}'.format(key): val for key, val in  total_returns.iteritems()}
        output = {}
        output.update(participation)
        output.update(win_percent)
        output.update(total_returns)
        return output
