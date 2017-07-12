import numpy as np
import pandas as pd
import os
import datetime
import itertools
import pypyodbc
from ram.strategy.intraday_reversion.src.trade_signals import *
from ram.strategy.base import Strategy
from ram.data.data_handler_sql import DataHandlerSQL
from ram.strategy.intraday_reversion.src.intraday_return_simulator import IntradayReturnSimulator

INTRADAY_DATA = os.path.join(os.getenv('DATA'), 'ram', 'intraday_src')

def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]

class IntradayReversion(Strategy):
    args1 = make_arg_iter({'n_estimators': [100],
                            'min_samples_split': [75],
                            'min_samples_leaf': [20]})

    args2 = make_arg_iter({'zLim': [.5],
                            'gapDownSampleTrim1': [.2],
                            'gapDownSampleTrim2': [.4],
                            'gapUpSampleTrim1': [.4],
                            'gapUpSampleTrim2': [.2]})

    args3 = make_arg_iter(
    {'SPY':[(.01, .002), (.01, .002)],
    'IWM':[(.01, .004), (.01, .002)],
    'QQQ':[(.01, .004), (.01, .004)],
    'VXX':[(.01, .01), (.01, .007)]})

    def get_column_parameters(self):
        output_params = {}
        for col_ind, (x, y, z) in enumerate(itertools.product(
            self.args1, self.args2, self.args3)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

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
    
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 
    def run_index(self, index):

        data = self.read_data_from_index(index)
        data = format_raw_data(data)

        i = 0
        for a1 in self.args1:
            predictions = get_predictions(data, **a1)

            for a2 in self.args2:
                signals = get_trade_signals(predictions, **a2)

                for a3 in self.args3:
                    returns = irs.get_returns(signals, **a3)
                    self._capture_output(returns, i)
                    i += 1
        self.write_index_results(self.output_returns, index)
        '''
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
            allTrades[i] = allTrades.sum(axis=1)
            output_results = output_results.join(allTrades[[ind]],
                                                 how='outer')
            i += 1
            print i
        self.write_index_results(output_results, index)'''
        return


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
            cost = .007 / nomClose

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
            rets[i] = ret - cost

        tkrData.loc[tkrData.Signal != 0, 'Ret'] = rets
        tkrData.Ret.fillna(0., inplace=True)
        tkrData = tkrData.loc[tkrData.Date >= start_dt,
                              ['Ticker','Date','Signal','Ret']]
        tkrData.index = tkrData.Date
        return tkrData

    def get_live_trades(self, params=None):
        zLim = .5
        rfcParams = {'n_estimators':100, 'min_samples_split':75,
                  'min_samples_leaf':20}
        gapDownParams = {'sampleLimLow':.25, 'sampleLimHigh':.25}
        gapUpParams = {'sampleLimLow':.25, 'sampleLimHigh':.25}
        
        datahandler = DataHandlerSQL()
        today = datetime.date.today()

        SQLCommandDate = ("select max(T0) from ram.dbo.ram_trading_dates "
                          "where T0 < '{0}/{1}/{2}'".format(
                            today.month, today.day, today.year))
        priorDate = datahandler.sql_execute(SQLCommandDate)[0][0].date()

        live_features = [
            'AdjVolume', 'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'RClose',
            'LAG1_AdjOpen','LAG1_AdjClose', 'LAG2_AdjOpen', 'LAG2_AdjClose',
            'LAG3_AdjOpen', 'LAG3_AdjClose', 'LAG4_AdjOpen', 'LAG4_AdjClose',
            'LAG5_AdjOpen', 'LAG5_AdjClose', 'LAG6_AdjOpen', 'LAG6_AdjClose',
            'LAG7_AdjOpen', 'LAG7_AdjClose', 'LAG8_AdjOpen', 'LAG8_AdjClose',
            'LAG9_AdjOpen', 'LAG9_AdjClose', 'LAG10_AdjOpen', 'LAG10_AdjClose',
            'VOL90_AdjClose','VOL10_AdjClose', 'PRMA10_AdjClose',
            'PRMA20_AdjClose', 'PRMA50_AdjClose', 'PRMA200_AdjClose',
            'RSI10', 'RSI30', 'MFI10', 'MFI30']

        train = datahandler.get_etf_data(
            self.get_ids_filter_args()['ids'],
            self.get_features(),
            self.get_ids_filter_args()['start_date'],
            today
            )
        live = datahandler.get_etf_data(
            self.get_ids_filter_args()['ids'],
            live_features,
            priorDate,
            today
            )
        live.Date = today
        livePriorClose = live[['SecCode','RClose']].copy()
        live.drop('RClose', axis=1, inplace=True)

        for col in live_features[::-1]:
            if col.find('LAG') < 0:
                live.rename(columns={col:'LAG1_{}'.format(col)}, inplace=True)
            else:
                ix1 = col.find('LAG') + 3
                ix2 = col.find('_')
                lix = int(col[ix1:ix2]) + 1
                live.rename(columns={col:'LAG{0}_{1}'.format(
                    lix, col[ix2+1:])},inplace=True)

        train.Date = [x.date() for x in train.Date]
        live['AdjOpen'] = -9999.
        live['AdjClose'] = -9999.
        data = train.append(live)
        data.reset_index(drop=True, inplace=True)
        data = self.create_features(data)
        data['pred'] = self.get_preds(data, trainLen=6, **rfcParams)

        train = data[(data.Date < today) & (data.pred.notnull())].copy()
        trainDwn = train.loc[train.zOpen < -zLim,
                             ['Ticker', 'Date', 'pred','DayRet']].copy()
        trainDwn.sort_values(by='pred', inplace=True)
        predGapDwn = self.get_pred_thresh(trainDwn, **gapDownParams)
        
        trainUp = train.loc[train.zOpen > zLim,
                             ['Ticker', 'Date', 'pred','DayRet']].copy()
        trainUp.sort_values(by='pred', inplace=True)
        predGapUp = self.get_pred_thresh(trainUp, **gapUpParams)
        
        livePreds = data.loc[data.Date == today,
                              ['Ticker', 'SecCode','LAG1_VOL90_AdjClose',
                               'pred','Date']].reset_index(drop=True)
        livePreds = livePreds.merge(livePriorClose)
        livePreds['OpenRetMin'] = zLim * livePreds.LAG1_VOL90_AdjClose
        livePreds['OpenPriceMinUp'] = livePreds.RClose * (1 + livePreds.OpenRetMin)
        livePreds['OpenPriceMinDwn'] = livePreds.RClose * (1 - livePreds.OpenRetMin)
        livePreds['predLimGapDown'] = predGapDwn
        livePreds['predLimGapUp'] = predGapUp
        livePreds['OpenUpSide'] = np.where(livePreds.pred >= predGapUp, 'Long',
                                           'Short')
        livePreds['OpenDownSide'] = np.where(livePreds.pred >= predGapDwn, 'Long',
                                           'Short')

        return livePreds

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
    parser.add_argument(
        '-l', '--live', action='store_true',
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
    elif args.live:
        strategy = IntradayReversion('version_0001', False)
        trades = strategy.get_live_trades()
        trades.to_csv('C:/temp/IntradayTrades.csv', index=False)
        # Send to Trade Engine


if __name__ == '__main__':
    main()
