import numpy as np
import pandas as pd
import datetime
from tqdm import tqdm
from gearbox import create_time_index
from sklearn.ensemble import RandomForestClassifier


# ~~~~~~ Modeling ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_predictions(data,
                    n_estimators = 100,
                    min_samples_split = 75,
                    min_samples_leaf = 20):

    data = data.copy()
    qtrIdxs = data.QIndex.sort_values().unique()[6:]
    features = list(data.columns.difference([
        'SecCode', 'Date', 'AdjOpen','AdjClose', 'LAG1_AdjVolume',
        'LAG1_AdjOpen','LAG1_AdjHigh','LAG1_AdjLow','LAG1_AdjClose',
        'LAG1_VOL90_AdjClose', 'LAG1_VOL10_AdjClose', 'Ticker', 'QIndex',
        'OpenRet', 'DayRet', 'zOpen', 'DoW', 'Day','Month','Qtr',
        'pred', 'Signal']))

    rfc = RandomForestClassifier(n_estimators = n_estimators,
                                 min_samples_split = min_samples_split,
                                 min_samples_leaf= min_samples_leaf,
                                 random_state=123)

    for qtr in tqdm(qtrIdxs):
        train_X = data.loc[data.QIndex < qtr, features]
        train_y = data.loc[data.QIndex < qtr, 'DayRet'] > 0
        test_X = data.loc[data.QIndex == qtr, features]
        
        rfc.fit(train_X, train_y)
        testPreds = rfc.predict_proba(test_X)[:, 1]
        data.loc[data.QIndex == qtr, 'Est'] = testPreds

    return data[['Ticker', 'Date', 'Est', 'zOpen', 'DayRet']].copy()


# ~~~~~~ Trade Signals ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_trade_signals(predictions, zLim=.5,
                      gapDownSampleTrim1 = .25, gapDownSampleTrim2 = .25,
                      gapUpSampleTrim1 = .25, gapUpSampleTrim2 = .25):

    # HARDCODED START DATE TIED TO START OF INTRADAY DATA
    start_dt = datetime.date(2007, 4, 25)

    data = predictions.dropna()
    signalDates = data.loc[(data.Date >= start_dt) &
                            (np.abs(data.zOpen) > zLim), 'Date']

    for dt in tqdm(signalDates.sort_values().unique()):
        # GAP DOWN
        train = data.loc[(data.zOpen < 0) & (data.Date < dt),
                            ['Est', 'DayRet']].copy()
        threshLim = thresh_optim(train, gapDownSampleTrim1, gapDownSampleTrim2)
        predictions.loc[predictions.Date == dt, 'threshLimDown'] = threshLim
        # GAP UP
        train = data.loc[(data.zOpen > 0) & (data.Date < dt),
                            ['Est', 'DayRet']].copy()
        threshLim = thresh_optim(train, gapUpSampleTrim1, gapUpSampleTrim2)
        predictions.loc[predictions.Date == dt, 'threshLimUp'] = threshLim

    gapDwnRev = ((predictions.zOpen <= -zLim) &
                        (predictions.Est  >= predictions.threshLimDown))
    gapDwnMo = ((predictions.zOpen <= -zLim) &
                        (predictions.Est < predictions.threshLimDown))
    gapUpRev = ((predictions.zOpen >= zLim) &
                        (predictions.Est  <= predictions.threshLimUp))
    gapUpMo = ((predictions.zOpen >= zLim) &
                        (predictions.Est  > predictions.threshLimUp))
    
    predictions['signal'] = np.where((gapUpRev) | (gapDwnMo), -1,
                                np.where((gapDwnRev) | (gapUpMo), 1, 0))

    return predictions[['Ticker', 'Date', 'DayRet', 'zOpen', 'signal']]

def thresh_optim(trainData, sampleLimLow, sampleLimHigh):
    trainData.sort_values(by='Est', inplace=True)
    obsN = np.arange(1, len(trainData) + 1)
    trainData['pctDwnBlw'] = ((trainData.DayRet < 0).cumsum() / obsN)
    trainData['pctUpAbv'] = ((trainData.DayRet[::-1] > 0).cumsum()[::-1] /
        (obsN[::-1]))
    trainData['winPctSum'] = trainData.pctDwnBlw + trainData.pctUpAbv
    nTrimL = np.round(len(trainData) * sampleLimLow, 0).astype(int)
    nTrimH = np.round(len(trainData) * sampleLimHigh, 0).astype(int)
    maxRow = trainData[nTrimL : -nTrimH].sort_values(
        'winPctSum', ascending=False).iloc[0]
    return maxRow.Est



# ~~~~~~ Data Processing ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

seccode_ticker_map = {
    '37591': 'IWM',
    '49234': 'QQQ',
    '61494': 'SPY',
    '10902726': 'VXX',
    '19753': 'DIA',
    '72954': 'KRE'
}

def format_raw_data(data):
    data = data.merge(pd.DataFrame(seccode_ticker_map.items(),
                                   columns=['SecCode', 'Ticker']))

    data['OpenRet'] = ((data.AdjOpen - data.LAG1_AdjClose) /
                        data.LAG1_AdjClose)
    data['DayRet'] = (data.AdjClose - data.AdjOpen) / data.AdjOpen
    data['zOpen'] = data.OpenRet / data.LAG1_VOL90_AdjClose

    data['QIndex'] = create_time_index(data.Date)
    data = _create_seasonal_vars(data)
    data = _create_pricing_vars(data)
    data = _get_momentum_indicator(data)

    tickers = data.Ticker.unique()
    for tkr in tickers:
        data['b{}'.format(tkr)] = np.where(data.Ticker == tkr, 1, 0)

    data = data.dropna()
    data.reset_index(drop=True, inplace=True)
    return data

def _create_seasonal_vars(data):
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

def _create_pricing_vars(data):
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

def _get_momentum_indicator(data):
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
