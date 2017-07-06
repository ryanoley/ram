import os
import pypyodbc
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from tqdm import tqdm

from ram.strategy.base import Strategy
from ram.data.data_handler_sql import DataHandlerSQL

from gearbox import create_time_index

from sklearn.ensemble import RandomForestClassifier

from ram.strategy.intraday_reversion.src.import_data import *
from ram.strategy.intraday_reversion.src.take_stop_returns import *


INTRADAY_DATA = os.path.join(os.getenv('DATA'), 'ram', 'intraday_src')


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


class IntradayReversion(Strategy):

    seccode_ticker_map = {
        37591: 'IWM',
        49234: 'QQQ',
        61494: 'SPY',
        10902726: 'VXX',
        19753: 'DIA',
        72954: 'KRE'
    }

    args1 = make_arg_iter({
        'n_estimators': [100],
        'min_samples_split': [75],
        'min_samples_leaf': [20]
    })

    args2 = make_arg_iter({
        'zLim': [.35],
        'dwnPctLim1': [.2, .4],
        'dwnPctLim2': [.2, .4],
        'upPctLim1': [.2, .4],
        'upPctLim2': [.2, .4]
    })

    args3 = make_arg_iter({
        'SPY': [(.007, .002), (.01, .002)],
        'IWM': [(.007, .002), (.01, .002)],
        'QQQ': [(.007, .002), (.01, .004)],
        'VXX': [(.01, .01), (.01, .007)]
    })

    def get_column_parameters(self):
        output_params = {}
        for col_ind, (x, y, z) in enumerate(
                itertools.product(self.args1, self.args2, self.args3)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run_index(self, index):

        data = self.read_data_from_index(index)
        data = self.create_features(data)

        ind = 0
        output_results = pd.DataFrame()

        for a1 in self.args1:
            self.get_preds(data, trainLen=6, **a1)

            for a2 in self.args2:
                data['Signal'] = self.get_trade_signals(
                    data, start_dt=min_dt, **a2)

                for a3 in self.args3:
                    allTrades = pd.DataFrame(
                        index=data.loc[data.Date >= min_dt,
                                       'Date'].sort_values().unique())
                    for tkr in tickers:
                        tData = data[data.Ticker == tkr].copy()
                        tIData = idata[idata.Ticker == tkr].copy()
                        exitRet, stopRet = a3[tkr]
                        tkr_results = self.get_tkr_returns(tData, tIData,
                                                           exitRet, stopRet)
                        allTrades[tkr] = tkr_results.Ret

                    allTrades.fillna(0., inplace=True)
                    allTrades[ind] = allTrades.sum(axis=1)
                    output_results = output_results.join(allTrades[[ind]],
                                                         how='outer')
                    ind += 1

        self.write_index_results(output_results, index)
        return

    # ~~~~~~ Predictive model data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def create_features(self, data):
        data.SecCode = data.SecCode.astype(int)
        data.sort_values(by=['SecCode', 'Date'], inplace=True)
        data['Ticker'] = [self.seccode_ticker_map[x] for x in data.SecCode]
        data['QIndex'] = create_time_index(data.Date)

        data['OpenRet'] = ((data.AdjOpen - data.LAG1_AdjClose) /
                           data.LAG1_AdjClose)
        data['DayRet'] = (data.AdjClose - data.AdjOpen) / data.AdjOpen
        data['zOpen'] = data.OpenRet / data.LAG1_VOL90_AdjClose

        data = self.create_seasonal_vars(data)
        data = self.create_pricing_vars(data)
        data = self.get_momentum_indicator(data)

        tickers = data.Ticker.unique()
        for tkr in tickers:
            tDates = data[data.Ticker == tkr].Date.shift(1)
            data.loc[data.Ticker == tkr, 'PriorDate'] = tDates
            data['b{}'.format(tkr)] = np.where(data.Ticker == tkr, 1, 0)

        data = data.dropna()
        data.reset_index(drop=True, inplace=True)
        return data

    def create_seasonal_vars(self, data):
        data['DoW'] = [x.weekday() for x in data.Date]
        data['Day'] = [x.day for x in data.Date]
        data['Month'] = [x.month for x in data.Date]
        data['Qtr'] = np.array([(m - 1) / 3 + 1 for m in data.Month])

        data['Q1'] = data.Qtr == 1
        data['Q2'] = data.Qtr == 2
        data['Q3'] = data.Qtr == 3
        data['Q4'] = data.Qtr == 4

        data['DoW0'] = data.DoW == 0
        data['DoW1'] = data.DoW == 1
        data['DoW2'] = data.DoW == 2
        data['DoW3'] = data.DoW == 3
        data['DoW4'] = data.DoW == 4

        return data

    def create_pricing_vars(self, data):
        out = pd.DataFrame([])
        for tkr in data.Ticker.unique():
            tdata = data[data.Ticker == tkr].copy()
            tdata['Min5'] = (tdata.LAG1_AdjClose ==
                             tdata.LAG1_AdjClose.rolling(window=5).min())
            tdata['Min10'] = (tdata.LAG1_AdjClose ==
                              tdata.LAG1_AdjClose.rolling(window=10).min())
            tdata['Min20'] = (tdata.LAG1_AdjClose ==
                              tdata.LAG1_AdjClose.rolling(window=20).min())

            tdata['Max5'] = (tdata.LAG1_AdjClose ==
                             tdata.LAG1_AdjClose.rolling(window=5).max())
            tdata['Max10'] = (tdata.LAG1_AdjClose ==
                              tdata.LAG1_AdjClose.rolling(window=10).max())
            tdata['Max20'] = (tdata.LAG1_AdjClose ==
                              tdata.LAG1_AdjClose.rolling(window=20).max())

            tdata['AbvPRMA10'] = tdata.LAG1_PRMA10_AdjClose > 0.
            tdata['BlwPRMA10'] = tdata.LAG1_PRMA10_AdjClose < 0.
            tdata['AbvPRMA20'] = tdata.LAG1_PRMA20_AdjClose > 0.
            tdata['BlwPRMA20'] = tdata.LAG1_PRMA20_AdjClose < 0.
            tdata['AbvPRMA50'] = tdata.LAG1_PRMA50_AdjClose > 0.
            tdata['BlwPRMA50'] = tdata.LAG1_PRMA50_AdjClose < 0.
            tdata['AbvPRMA200'] = tdata.LAG1_PRMA200_AdjClose > 0.
            tdata['BlwPRMA200'] = tdata.LAG1_PRMA200_AdjClose < 0.

            tdata['Spread'] = ((tdata.LAG1_AdjHigh - tdata.LAG1_AdjLow) /
                               tdata.LAG1_AdjOpen)
            tdata['MaxSpread5'] = (tdata.Spread ==
                                   tdata.Spread.rolling(window=5).max())
            tdata['MaxSpread10'] = (tdata.Spread ==
                                    tdata.Spread.rolling(window=10).max())
            tdata['MinSpread5'] = (tdata.Spread ==
                                   tdata.Spread.rolling(window=5).min())
            tdata['MinSpread10'] = (tdata.Spread ==
                                    tdata.Spread.rolling(window=10).min())
            tdata['SRMA20'] = (tdata.Spread /
                               tdata.Spread.rolling(window=20).mean())
            tdata['AbvSRMA20'] = tdata.SRMA20 > 1.
            tdata['BlwSRMA20'] = tdata.SRMA20 < 1.

            tdata['MinVolume5'] = (
                tdata.LAG1_AdjVolume == tdata.LAG1_AdjVolume.rolling(
                    window=5).min())
            tdata['MinVolume10'] = (
                tdata.LAG1_AdjVolume == tdata.LAG1_AdjVolume.rolling(
                    window=10).min())
            tdata['MinVolume20'] = (
                tdata.LAG1_AdjVolume == tdata.LAG1_AdjVolume.rolling(
                    window=20).min())
            tdata['MaxVolume5'] = (
                tdata.LAG1_AdjVolume == tdata.LAG1_AdjVolume.rolling(
                    window=5).max())
            tdata['MaxVolume10'] = (
                tdata.LAG1_AdjVolume == tdata.LAG1_AdjVolume.rolling(
                    window=10).max())
            tdata['MaxVolume20'] = (
                tdata.LAG1_AdjVolume == tdata.LAG1_AdjVolume.rolling(
                    window=20).max())
            tdata['VRMA5'] = (
                tdata.LAG1_AdjVolume / tdata.LAG1_AdjVolume.rolling(
                    window=5).mean())
            tdata['VRMA10'] = (
                tdata.LAG1_AdjVolume / tdata.LAG1_AdjVolume.rolling(
                    window=10).mean())

            tdata['zScore5'] = (
                (tdata.LAG1_AdjClose - tdata.LAG5_AdjClose) /
                tdata.LAG5_AdjClose) / tdata.LAG1_VOL90_AdjClose
            out = out.append(tdata)

        return out

    def get_momentum_indicator(self, data):
        momInd = np.zeros(data.shape[0])
        for i in range(10, 0, -1):
            posMo = ((data['LAG{}_AdjClose'.format(i+1)] <= data['LAG{}_AdjOpen'.format(i)]) &
                     (data['LAG{}_AdjOpen'.format(i)] <= data['LAG{}_AdjClose'.format(i)]))
            negMo = ((data['LAG{}_AdjClose'.format(i+1)] >= data['LAG{}_AdjOpen'.format(i)]) &
                     (data['LAG{}_AdjOpen'.format(i)] >= data['LAG{}_AdjClose'.format(i)]))
            momInd += np.where(
                (posMo) | (negMo),
                np.abs((data['LAG{}_AdjClose'.format(i)] - data['LAG{}_AdjOpen'.format(i)]) / data['LAG{}_AdjOpen'.format(i)]),
                -np.abs((data['LAG{}_AdjClose'.format(i)] - data['LAG{}_AdjOpen'.format(i)]) / data['LAG{}_AdjOpen'.format(i)]))
            data.drop(labels=['LAG{}_AdjClose'.format(i+1),
                              'LAG{}_AdjOpen'.format(i+1)], axis=1, inplace=True)
        data['momInd'] = momInd
        return data

    def get_preds(self, pdata, trainLen=6, n_estimators=100,
                  min_samples_split=75, min_samples_leaf=20):

        qtrIdxs = np.unique(pdata.QIndex)[trainLen:]

        features = list(pdata.columns.difference([
            'SecCode', 'Date', 'AdjOpen', 'AdjClose', 'LAG1_AdjVolume',
            'LAG1_AdjOpen', 'LAG1_AdjHigh', 'LAG1_AdjLow', 'LAG1_AdjClose',
            'LAG1_VOL90_AdjClose', 'LAG1_VOL10_AdjClose', 'Ticker', 'QIndex',
            'OpenRet', 'DayRet', 'zOpen', 'DoW', 'Day', 'Month', 'Qtr',
            'PriorDate', 'pred', 'Signal'
        ]))

        clf = RandomForestClassifier(n_estimators=n_estimators,
                                     min_samples_split=min_samples_split,
                                     min_samples_leaf=min_samples_leaf,
                                     random_state=123, n_jobs=-1)

        for qtr in tqdm(qtrIdxs):
            train_X = pdata.loc[pdata.QIndex < qtr, features]
            train_y = pdata.loc[pdata.QIndex < qtr, 'DayRet'] > 0
            test_X = pdata.loc[pdata.QIndex == qtr, features]

            clf.fit(X=train_X, y=train_y)

            testPreds = clf.predict_proba(test_X)[:, 1]
            pdata.loc[pdata.QIndex == qtr, 'pred'] = testPreds

    def get_trade_signals(self, data, zLim=.5, thresh_buffer=0.0):
        """
        At what point is the prediction value a good long or short
        """
        tradeSignals = np.where((data.zOpen > zLim), -1, np.where((data.zOpen < -zLim), 1, 0))
        return tradeSignals

    def get_ticker_returns(self, tkrData, intraTkrData, take_perc, stop_perc):
        return None

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        """
        Overriden method from Strategy
        """
        return ['AdjOpen', 'AdjClose', 'LAG1_AdjVolume', 'LAG1_AdjOpen',
                'LAG1_AdjHigh', 'LAG1_AdjLow', 'LAG1_AdjClose',
                'LAG2_AdjOpen', 'LAG2_AdjClose', 'LAG3_AdjOpen',
                'LAG3_AdjClose', 'LAG4_AdjOpen', 'LAG4_AdjClose',
                'LAG5_AdjOpen', 'LAG5_AdjClose', 'LAG6_AdjOpen',
                'LAG6_AdjClose', 'LAG7_AdjOpen', 'LAG7_AdjClose',
                'LAG8_AdjOpen', 'LAG8_AdjClose', 'LAG9_AdjOpen',
                'LAG9_AdjClose', 'LAG10_AdjOpen', 'LAG10_AdjClose',
                'LAG11_AdjOpen', 'LAG11_AdjClose',
                'LAG1_VOL90_AdjClose', 'LAG1_VOL10_AdjClose',
                'LAG1_PRMA10_AdjClose', 'LAG1_PRMA20_AdjClose',
                'LAG1_PRMA50_AdjClose', 'LAG1_PRMA200_AdjClose',
                'LAG1_RSI10', 'LAG1_RSI30',
                'LAG1_MFI10', 'LAG1_MFI30']

    def get_ids_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {
            'ids': ['SPY', 'QQQ', 'IWM', 'VXX'],
            'start_date': '2002-04-24',
            'end_date': '2017-06-07'}

    def get_constructor_type(self):
        '''
        Overriden method from Strategy
        '''
        return 'etfs'


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(IntradayReversion)
