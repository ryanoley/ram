import numpy as np
import pandas as pd
import os
import pypyodbc
import datetime
import itertools
from tqdm import tqdm

from ram.strategy.base import Strategy
from gearbox import create_time_index

from sklearn.ensemble import RandomForestClassifier


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]

class IntradayReversion(Strategy):

    secCode_ticker = {37591:'IWM', 49234:'QQQ', 61494:'SPY',
                      10902726:'VXX', 19753:'DIA', 72954:'KRE'}
    '''
    args1 = make_arg_iter({'n_estimators': [25, 50, 75],
                            'min_samples_split': [25, 50, 75],
                            'min_samples_leaf': [5, 10, 20]})

    args2 = make_arg_iter({'zLim': [.35, .50, .65],
                            'dwnPctLim1': [.1, .25, .4],
                            'dwnPctLim2': [.1, .25, .4],
                            'upPctLim1': [.1, .25, .4],
                            'upPctLim2': [.1, .25, .4]})
                            
    args3 = [
        {'SPY':(.007,.002), 'IWM':(.007,.002), 'QQQ':(.007,.002), 'VXX':(.01,.01)},
        {'SPY':(.007,.004), 'IWM':(.007,.004), 'QQQ':(.007,.004), 'VXX':(.01,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.01)},
        {'SPY':(.01,.002), 'IWM':(.01,.002), 'QQQ':(.01,.002), 'VXX':(.01,.01)},
        {'SPY':(.01,.004), 'IWM':(.01,.04), 'QQQ':(.01,.004), 'VXX':(.01,.01)},
        {'SPY':(.01,.006), 'IWM':(.01,.06), 'QQQ':(.01,.006), 'VXX':(.01,.01)},
        {'SPY':(.005,.002), 'IWM':(.005,.002), 'QQQ':(.005,.002), 'VXX':(.01,.01)},
        {'SPY':(.005,.004), 'IWM':(.005,.04), 'QQQ':(.005,.004), 'VXX':(.01,.01)},
        {'SPY':(.005,.006), 'IWM':(.005,.06), 'QQQ':(.005,.006), 'VXX':(.01,.01)},
        {'SPY':(.007,.002), 'IWM':(.007,.002), 'QQQ':(.007,.002), 'VXX':(.0075,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.015,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.0075)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.01)},
        {'SPY':(.007,.004), 'IWM':(.007,.004), 'QQQ':(.007,.004), 'VXX':(.01,.015)}
        ]
        
    '''
    args1 = make_arg_iter({'n_estimators': [50],
                            'min_samples_split': [25],
                            'min_samples_leaf': [10]})

    args2 = make_arg_iter({'zLim': [.50],
                            'dwnPctLim1': [.1],
                            'dwnPctLim2': [.4],
                            'upPctLim1': [.4],
                            'upPctLim2': [.1]})

    args3 = [
        {'SPY':(.007,.002), 'IWM':(.007,.002), 'QQQ':(.007,.002), 'VXX':(.01,.01)},
        {'SPY':(.007,.004), 'IWM':(.007,.004), 'QQQ':(.007,.004), 'VXX':(.01,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.01)},
        {'SPY':(.01,.002), 'IWM':(.01,.002), 'QQQ':(.01,.002), 'VXX':(.01,.01)},
        {'SPY':(.01,.004), 'IWM':(.01,.04), 'QQQ':(.01,.004), 'VXX':(.01,.01)},
        {'SPY':(.01,.006), 'IWM':(.01,.06), 'QQQ':(.01,.006), 'VXX':(.01,.01)},
        {'SPY':(.005,.002), 'IWM':(.005,.002), 'QQQ':(.005,.002), 'VXX':(.01,.01)},
        {'SPY':(.005,.004), 'IWM':(.005,.04), 'QQQ':(.005,.004), 'VXX':(.01,.01)},
        {'SPY':(.005,.006), 'IWM':(.005,.06), 'QQQ':(.005,.006), 'VXX':(.01,.01)},
        {'SPY':(.007,.002), 'IWM':(.007,.002), 'QQQ':(.007,.002), 'VXX':(.0075,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.015,.01)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.0075)},
        {'SPY':(.007,.006), 'IWM':(.007,.006), 'QQQ':(.007,.006), 'VXX':(.01,.01)},
        {'SPY':(.007,.004), 'IWM':(.007,.004), 'QQQ':(.007,.004), 'VXX':(.01,.015)}
        ]


    def get_column_parameters(self):
        output_params = {}
        for col_ind, (x, y, z) in enumerate(itertools.product(
            self.args1, self.args2, self.args3)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

    def run_index(self, index):
        data = self.read_data_from_index(index)
        data = self.create_features(data)
        tickers = data.Ticker.unique()
        idata = self.create_intraday_data(tickers,
                                          intraday_dir='C:/temp/intraday_src')
        min_dt = idata.Date.min()
      
        output_results = pd.DataFrame()
        ind = 0
        for a1 in self.args1:
            data['pred'] = self.get_preds(data, trainLen=6, **a1)

            for a2 in self.args2:
                data['Signal'] = self.get_trd_signals(data, start_dt = min_dt,
                                                      **a2)
                for a3 in self.args3:
                    allTrades = pd.DataFrame(index = data.loc[data.Date >= min_dt,
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

    def create_features(self, data):
        data.SecCode = data.SecCode.astype(int)
        data.sort_values(by=['SecCode','Date'], inplace=True)
        data['Ticker'] = [self.secCode_ticker[x] for x in data.SecCode]
        data['QIndex'] = create_time_index(data.Date)

        data['OpenRet'] = ((data.AdjOpen - data.LAG1_AdjClose) /
                            data.LAG1_AdjClose)
        data['DayRet'] = (data.AdjClose - data.AdjOpen) / data.AdjOpen
        data['zOpen'] = data.OpenRet / data.LAG1_VOL90_AdjClose

        data = self.create_seas_vars(data)
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
    
    def create_seas_vars(self, data):
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
            tdata = data[data.Ticker==tkr].copy()
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
        
            tdata['MinVolume5'] = (tdata.LAG1_AdjVolume ==
                                        tdata.LAG1_AdjVolume.rolling(window=5
                                                                     ).min())
            tdata['MinVolume10'] = (tdata.LAG1_AdjVolume ==
                                        tdata.LAG1_AdjVolume.rolling(window=10
                                                                     ).min())
            tdata['MinVolume20'] = (tdata.LAG1_AdjVolume ==
                                        tdata.LAG1_AdjVolume.rolling(window=20
                                                                     ).min())
            tdata['MaxVolume5'] = (tdata.LAG1_AdjVolume ==
                                        tdata.LAG1_AdjVolume.rolling(window=5
                                                                     ).max())
            tdata['MaxVolume10'] = (tdata.LAG1_AdjVolume ==
                                        tdata.LAG1_AdjVolume.rolling(window=10
                                                                     ).max())
            tdata['MaxVolume20'] = (tdata.LAG1_AdjVolume ==
                                        tdata.LAG1_AdjVolume.rolling(window=20
                                                                     ).max())
            tdata['VRMA5'] = (tdata.LAG1_AdjVolume /
                                tdata.LAG1_AdjVolume.rolling(window=5).mean())
            tdata['VRMA10'] = (tdata.LAG1_AdjVolume /
                                tdata.LAG1_AdjVolume.rolling(window=10).mean())
            
            tdata['zScore5'] = ((tdata.LAG1_AdjClose - tdata.LAG5_AdjClose)  /
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
            momInd += np.where((posMo) | (negMo),
                np.abs((data['LAG{}_AdjClose'.format(i)] - data['LAG{}_AdjOpen'.format(i)]) / data['LAG{}_AdjOpen'.format(i)]),
                -np.abs((data['LAG{}_AdjClose'.format(i)] - data['LAG{}_AdjOpen'.format(i)]) / data['LAG{}_AdjOpen'.format(i)]))
            data.drop(labels=['LAG{}_AdjClose'.format(i+1),
                              'LAG{}_AdjOpen'.format(i+1)], axis=1, inplace=True)
        data['momInd'] = momInd
        return data

    def create_intraday_data(self, tickers,
                             intraday_dir='C:/temp/intraday_src'):
        if intraday_dir is None:
            SQLCommand = ("SELECT * from ram.dbo.IntradayPricing" +
                          " WHERE Ticker in {}".format(str(tuple(tickers))))
            connection = pypyodbc.connect('Driver={SQL Server};Server=QADIRECT;'
                                      'Database=ram;uid=ramuser;pwd=183madison')
            cursor = connection.cursor()
            cursor.execute(SQLCommand)
            idata = pd.DataFrame(cursor.fetchall(),
                                 columns=['Ticker', 'DateTime', 'High', 'Low',
                                          'Open', 'Close', 'Volume',
                                          'OpenInterest'])
            connection.close()
        else:
            idata = pd.DataFrame([])
            for tkr in tickers:
                fl_path = os.path.join(intraday_dir, tkr + '.csv.')
                tdata = pd.read_csv(fl_path)
                idata = idata.append(tdata)

        idata.DateTime = pd.to_datetime(idata.DateTime)
        idata['Date'] = [x.date() for x in idata.DateTime]
        idata['Time'] = [x.time() for x in idata.DateTime]
        idata.rename(columns={'Volume':'CumVolume'}, inplace=True)
        idata.reset_index(drop=True, inplace=True)
        return idata

    def get_preds(self, data, trainLen = 6, n_estimators = 50,
                  min_samples_split = 30, min_samples_leaf = 10):
        pdata = data.copy()
        qtrIdxs = data.QIndex.sort_values().unique()[trainLen:]
        features = list(pdata.columns.difference([
            'SecCode', 'Date', 'AdjOpen','AdjClose', 'LAG1_AdjVolume',
            'LAG1_AdjOpen','LAG1_AdjHigh','LAG1_AdjLow','LAG1_AdjClose',
            'LAG1_VOL90_AdjClose', 'LAG1_VOL10_AdjClose', 'Ticker', 'QIndex',
            'OpenRet', 'DayRet', 'zOpen', 'DoW', 'Day','Month','Qtr',
            'PriorDate', 'pred', 'Signal']))

        rfModel = RandomForestClassifier(n_estimators = n_estimators,
                                         min_samples_split = min_samples_split,
                                          min_samples_leaf= min_samples_leaf,
                                          random_state=123)

        for i in tqdm(range(len(qtrIdxs))):
            qtr = qtrIdxs[i]
            train = pdata.loc[pdata.QIndex < qtr]
            test = pdata.loc[pdata.QIndex == qtr, features]
            rfModel.fit(train[features], train.DayRet > 0)
            testPreds = rfModel.predict_proba(test)[:,1]
            pdata.loc[pdata.QIndex == qtr, 'pred'] = testPreds

        return pdata.pred
    
    def get_trd_signals(self, data,
                        zLim=.5,
                        start_dt=datetime.date(2007, 4, 25),
                        dwnPctLim1 = .15, dwnPctLim2 = .4,
                        upPctLim1 = .4, upPctLim2 = .15):
        sdata = data[data.pred.notnull()].copy()
        allSignals = (sdata.Date >= start_dt) & (np.abs(sdata.zOpen) > zLim)
        evalDates = sdata.loc[allSignals, 'Date'].sort_values().unique()

        for i in tqdm(range(len(evalDates))):  
            dt = evalDates[i]
            trainDwn = sdata.loc[(sdata.zOpen < -zLim) & (sdata.Date < dt),
                                ['Ticker', 'Date', 'pred','DayRet']].copy()
            trainDwn.sort_values(by='pred', inplace=True)
            trainDwn.reset_index(drop=True, inplace=True)
            trainDwn['pctDwnBlw'] = ((trainDwn.DayRet < 0).cumsum() /
                                    trainDwn.index.values)
            trainDwn['pctUpAbv'] = ((trainDwn.DayRet[::-1] > 0).cumsum()[::-1] /
                                    (len(trainDwn) - trainDwn.index))
            trainDwn['diffInd'] = trainDwn.pctDwnBlw + trainDwn.pctUpAbv
            sampleLim1 = np.round(len(trainDwn) * dwnPctLim1, 0).astype(int)
            sampleLim2 = np.round(len(trainDwn) * dwnPctLim2, 0).astype(int)
            maxRow = trainDwn[sampleLim1 : -sampleLim2].sort_values(
                'diffInd', ascending=False).iloc[0]
            sdata.loc[sdata.Date == dt, 'predLimDown'] = maxRow.pred 

            trainUp = sdata.loc[(sdata.zOpen > zLim) & (sdata.Date < dt),
                                    ['Ticker', 'Date', 'pred','DayRet']].copy()
            trainUp.sort_values(by='pred', inplace=True)
            trainUp.reset_index(drop=True, inplace=True)
            trainUp['pctDwnBlw'] = ((trainUp.DayRet < 0).cumsum() /
                                    trainUp.index.values)
            trainUp['pctUpAbv'] = ((trainUp.DayRet[::-1] > 0).cumsum()[::-1] /
                                    (len(trainUp) - trainUp.index))
            trainUp['diffInd'] = trainUp.pctDwnBlw + trainUp.pctUpAbv
            sampleLim1 = np.round(len(trainUp) * upPctLim1, 0).astype(int)
            sampleLim2 = np.round(len(trainUp) * upPctLim2, 0).astype(int)
            maxRow = trainUp[sampleLim1 : -sampleLim2].sort_values(
                'diffInd', ascending=False).iloc[0]
            sdata.loc[sdata.Date == dt, 'predLimUp'] = maxRow.pred
    
        gapDwnRev = (sdata.zOpen <= -zLim) & (sdata.pred  >= sdata.predLimDown)
        gapDwnMo = (sdata.zOpen <= -zLim) & (sdata.pred < sdata.predLimDown)
        gapUpRev = (sdata.zOpen >= zLim) & (sdata.pred  <= sdata.predLimUp)
        gapUpMo = (sdata.zOpen >= zLim) & (sdata.pred  > sdata.predLimUp)
        
        tradeSignals = np.zeros(len(data))
        tradeSignals[data.pred.notnull().values] = np.where((gapUpRev) | (gapDwnMo),
            -1, np.where((gapDwnRev) | (gapUpMo), 1, 0))

        return tradeSignals

    def get_tkr_returns(self, tkrData, intraTkrData, exitRet, stopRet,
                      start_dt=datetime.date(2007, 4, 25)):
        
        trades = pd.DataFrame([])
        tradeRows = tkrData.loc[tkrData.Signal != 0,
                                ['Date', 'PriorDate', 'Signal']]
        rets = np.zeros(len(tradeRows))

        for i in tqdm(range(len(tradeRows))):
            row = tradeRows.iloc[i]
            intraTkrDateData = intraTkrData[intraTkrData.Date == row.Date]
            if (len(intraTkrDateData) == 0):
                continue
            nomOpen = intraTkrDateData.Open.iloc[0]
            nomClose = intraTkrDateData.Close.iloc[-1]

            if row.Signal == -1:
                prcExit = nomOpen * (1 - exitRet)
                prcStop = nomOpen * (1 + stopRet)
                exitBars = (intraTkrDateData.Low <= prcExit)
                stopBars = (intraTkrDateData.High >= prcStop)
            elif row.Signal == 1:
                prcExit = nomOpen * (1 + exitRet)
                prcStop = nomOpen * (1 - stopRet)
                exitBars = (intraTkrDateData.High >= prcExit)
                stopBars = (intraTkrDateData.Low <= prcStop)

            exitIx = intraTkrDateData[exitBars].index.min()
            stopIx = intraTkrDateData[stopBars].index.min()
            exitIx = np.inf if np.isnan(exitIx) else exitIx
            stopIx = np.inf if np.isnan(stopIx) else stopIx
            if (exitIx == stopIx) & (exitIx != np.inf):
                ret = 0.
            elif (exitIx <= stopIx) & (exitIx != np.inf):
                ret = exitRet
            elif stopIx < exitIx:
                ret = -stopRet
            else:
                ret = row.Signal * (nomClose - nomOpen) / nomOpen
            rets[i] = ret

        tkrData.loc[tkrData.Signal != 0, 'Ret'] = rets
        tkrData = tkrData.loc[tkrData.Date >= start_dt,
                              ['Ticker','Date','Signal','Ret']]
        tkrData.index = tkrData.Date
        return tkrData

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        '''
        Overriden method from Strategy
        '''
        return ['AdjOpen', 'AdjClose', 'LAG1_AdjVolume','LAG1_AdjOpen',
                'LAG1_AdjHigh', 'LAG1_AdjLow', 'LAG1_AdjClose',
                'LAG2_AdjOpen', 'LAG2_AdjClose', 'LAG3_AdjOpen',
                'LAG3_AdjClose', 'LAG4_AdjOpen', 'LAG4_AdjClose',
                'LAG5_AdjOpen', 'LAG5_AdjClose', 'LAG6_AdjOpen',
                'LAG6_AdjClose', 'LAG7_AdjOpen', 'LAG7_AdjClose',
                'LAG8_AdjOpen', 'LAG8_AdjClose', 'LAG9_AdjOpen',
                'LAG9_AdjClose', 'LAG10_AdjOpen', 'LAG10_AdjClose',
                'LAG11_AdjOpen', 'LAG11_AdjClose',
                'LAG1_VOL90_AdjClose','LAG1_VOL10_AdjClose',
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
            'start_date': '4/24/2002',
            'end_date': '06/07/2017'}
    
    def get_constructor_type(self):
        '''
        Overriden method from Strategy
        '''
        return 'etfs'



def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data', action='store_true',
        help='Run DataConstructor')
    parser.add_argument(
        '-w', '--write_simulation', action='store_true',
        help='Run simulatoin')
    parser.add_argument(
        '-s', '--simulation', action='store_true',
        help='Run simulatoin')
    args = parser.parse_args()

    if args.data:
        IntradayReversion().make_data()
    elif args.write_simulation:
        strategy = IntradayReversion('version_0001', True)
        strategy.start()
    elif args.simulation:
        strategy = IntradayReversion('version_0001', False)
        strategy.start()



if __name__ == '__main__':
    main()
    

