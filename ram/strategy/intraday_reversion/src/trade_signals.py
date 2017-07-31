import numpy as np
import pandas as pd
from tqdm import tqdm

from sklearn.ensemble import RandomForestClassifier
from ram.strategy.intraday_reversion.src.intraday_return_simulator import *
from ram.strategy.intraday_reversion.src.prediction_thresh_optim import prediction_thresh_optim


# ~~~~~~~~~~~ Model predictions ~~~~~~~~~~~~~~~~
def get_predictions(data,
                    intraday_simulator,
                    n_estimators=100,
                    min_samples_split=75,
                    min_samples_leaf=20):

    data = data.copy()
    responses = _create_response(data.Ticker.unique(), intraday_simulator)
    response_columns = responses.columns.difference(['Date','Ticker'])
    data = data.merge(responses)

    # HARD-CODED TRAINING PERIOD LENGTH
    qtr_indexes = np.unique(data.QIndex)[6:]

    features = list(data.columns.difference([
        'SecCode', 'Date', 'AdjOpen', 'AdjClose', 'LAG1_AdjVolume',
        'LAG1_AdjOpen', 'LAG1_AdjHigh', 'LAG1_AdjLow', 'LAG1_AdjClose',
        'LAG1_VOL90_AdjClose', 'LAG1_VOL10_AdjClose', 'Ticker', 'QIndex',
        'OpenRet', 'DayRet', 'zOpen', 'DoW', 'Day', 'Month', 'Qtr'] +
        list(responses.columns)))


    clf = RandomForestClassifier(n_estimators=n_estimators,
                                 min_samples_split=min_samples_split,
                                 min_samples_leaf=min_samples_leaf,
                                 random_state=123, n_jobs=-1)

    pred_columns = ['p_{}'.format(col) for col in response_columns]
    data = data.reindex(columns=list(data.columns) + pred_columns)

    print('\nTraining Predictive Models: ')
    for qtr in tqdm(qtrIdxs):
        train_X = data.loc[data.QIndex < qtr, features]
        train_y = data.loc[data.QIndex < qtr, response_columns]
        test_X = data.loc[data.QIndex == qtr, features]

        clf.fit(X=train_X, y=train_y)
        # ASSUMPTION: prediction = Long Prob - Short Prob
        probs = clf.predict_proba(test_X)
        long_ind = np.where(clf.classes_[0] == 1)[0][0]
        short_ind = np.where(clf.classes_[0] == -1)[0][0]

        preds = np.array([x[:,long_ind] - x[:,short_ind] for x in probs]).transpose()

        data.loc[data.QIndex == qtr, pred_columns] = preds

    downstream_features = ['zOpen']+ list(response_columns)
    return data[['Ticker', 'Date'] + pred_columns + downstream_features]


def _create_response(tickers, intraday_simulator):
    if isinstance(tickers, str):
        tickers = [tickers]
    # Create responses
    responses = pd.DataFrame([])
    for ticker in tickers:
        ticker_response = pd.DataFrame([])
        for take_stop, response in intraday_simulator._response_data[ticker].items():
            response = response.rename(columns = {'response':take_stop})
            ticker_response = pd.concat([ticker_response, response[take_stop]], axis=1)
        ticker_response['Ticker'] = ticker
        responses = responses.append(ticker_response)

    return responses.reset_index()
    
    
# ~~~~~~~~~~~ Trade signals / Thresh optim ~~~~~~~~~~~~~~~~

def get_trade_signals(predictions,
                      zLim=.5,
                      gap_down_limit_1=0.25,
                      gap_down_limit_2=0.25,
                      gap_up_limit_1=0.25,
                      gap_up_limit_2=0.25):
    predictions = predictions.copy()
    pred_cols = [x for x in predictions.columns if x.find('p_') > -1]
    for col in pred_cols:
        predictions['prediction'] = predictions[col].copy()
        predictions['response'] = predictions[col.replace('p_','')].copy()
        
        ## prediction_thresh_optim
        predictions[col.replace('p_','s_')] = prediction_thresh_optim(
            predictions,
            zLim,
            gap_down_limit_1,
            gap_down_limit_2,
            gap_up_limit_1,
            gap_up_limit_2)
    signal_cols = [x for x in predictions.columns if x.find('s_') > -1]
        
    return predictions[['Ticker', 'Date'] + signal_cols].copy()

def prediction_thresh_optim(data,
                            zLim=0.5,
                            gap_down_limit_1=0.25,
                            gap_down_limit_2=0.25,
                            gap_up_limit_1=0.25,
                            gap_up_limit_2=0.25):
    assert 'Date' in data
    assert 'prediction' in data
    assert 'response' in data
    assert 'zOpen' in data

    # Format for fast computation
    data_gap_down = data.loc[data.zOpen < -zLim].copy()
    data_gap_down.sort_values('prediction', inplace=True)
    data_gap_down = data_gap_down.dropna()

    data_gap_up = data.loc[data.zOpen > zLim].copy()
    data_gap_up.sort_values('prediction', inplace=True)
    data_gap_up = data_gap_up.dropna()

    eval_dates = np.unique(data.Date)
    eval_dates = eval_dates[eval_dates >= data_gap_down.Date.min()]
    eval_dates = eval_dates[eval_dates >= data_gap_up.Date.min()]

    # Ensure sufficient number of days of training data
    print('\nFitting prediction thresholds:')
    for date in tqdm(eval_dates[50:]):
        inds = data.Date == date

        data.loc[inds, 'gap_down_inflection'] = \
            _get_prediction_thresh(
                data_gap_down.loc[data_gap_down.Date < date],
                gap_down_limit_1, gap_down_limit_2)

        data.loc[inds, 'gap_up_inflection'] = \
            _get_prediction_thresh(
                data_gap_up.loc[data_gap_up.Date < date],
                gap_up_limit_1, gap_up_limit_2)

    return _get_trade_signals(data, zLim)


def _get_prediction_thresh(data,
                           gap_limit_low_side,
                           gap_limit_high_side):
    """
    DATA MUST BE PRE-SORTED BY PREDICTION COLUMN!!
    """
    n_obs = np.arange(1, len(data) + 1, dtype=np.float_)

    win_row_and_below = np.cumsum((data.response == -1).values) / n_obs

    win_above = (np.cumsum((data.response == 1).values[::-1]) / n_obs)[::-1]
    win_above = np.roll(win_above, -1)
    win_above[-1] = np.nan

    wins = win_row_and_below + win_above

    # Trim values from extremes to control odd behavior
    trim_low = int(len(data) * gap_limit_low_side)
    trim_high = int(len(data) * gap_limit_high_side)

    max_ind = np.argmax(wins[trim_low:-trim_high])
    data_ind = data.index[trim_low:-trim_high][max_ind]

    return data.prediction.loc[data_ind]


def _get_trade_signals(data, zLim):
    return np.where(
        # GAP DOWN SHORTS
        (data.prediction <= data.gap_down_inflection) &
        (data.zOpen < -zLim), -1, np.where(
        # GAP DOWN LONGS
        (data.prediction > data.gap_down_inflection) &
        (data.zOpen < -zLim), 1, np.where(
        # GAP UP SHORTS
        (data.prediction <= data.gap_up_inflection) &
        (data.zOpen > zLim), -1, np.where(
        # GAP UP LONGS
        (data.prediction > data.gap_up_inflection) &
        (data.zOpen > zLim), 1,
        # ELSE ZERO
        0))))
