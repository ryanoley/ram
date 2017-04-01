import os
import numpy as np
import pandas as pd
import datetime as dt


# ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _unpivot(data, value_name='Value'):
    out = pd.melt(data.reset_index(), id_vars='index')
    out.columns = ['Date', 'ColumnName', value_name]
    return out


# ~~~~~~ Response Variables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _get_ranks(return_data, n_days=20, thresh=10):
    ret_sums = return_data.rolling(n_days).sum()
    ret_ranks = ret_sums.rank(axis=1) / return_data.shape[1] * 100
    ret_responses = (ret_ranks >= (100-thresh)).shift(-n_days)
    return ret_responses


def Xmake_responses(return_data, n_days):
    out = _unpivot(_get_ranks(return_data, n_days, 5), 'Response_5')
    out = out.merge(_unpivot(_get_ranks(return_data, n_days, 10), 'Response_10'))
    out = out.merge(_unpivot(_get_ranks(return_data, n_days, 20), 'Response_20'))
    out = out.merge(_unpivot(_get_ranks(return_data, n_days, 30), 'Response_30'))
    return out


def make_responses(return_data, n_days):
    out = _unpivot(_get_returns(return_data, n_days), 'Response1')
    return out


def _get_returns(return_data, n_days):
    ret_sums = return_data.rolling(n_days).sum()
    ret_responses = (ret_sums > 0).shift(-n_days)
    return ret_responses






# ~~~~~~ Features ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_features_from_rets(return_data):
    out = _unpivot(return_data.rolling(10).mean() / return_data.rolling(10).std(), 'Sharpe10')
    out = out.merge(_unpivot(return_data.rolling(20).mean() / return_data.rolling(20).std(), 'Sharpe20'))
    out = out.merge(_unpivot(return_data.rolling(60).mean() / return_data.rolling(60).std(), 'Sharpe60'))
    out = out.merge(_unpivot(return_data.rolling(20).min(), 'Min20'))
    out = out.merge(_unpivot(return_data.rolling(60).min(), 'Min60'))
    out = out.merge(_unpivot(return_data.rolling(5).mean(), 'Mean5'))
    out = out.merge(_unpivot(return_data.rolling(10).mean(), 'Mean10'))
    out = out.merge(_unpivot(return_data.rolling(20).mean(), 'Mean20'))
    return out





# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def _get_some_spy_data():
    dh = DataHandlerSQL()
    spy_features = ['PRMA10_AdjClose', 'PRMA30_AdjClose', 'VOL10_AdjClose',
                    'VOL30_AdjClose', 'DISCOUNT50_AdjClose', 'RSI10_AdjClose',
                    'AdjClose', 'LAG5_AdjClose', 'LAG10_AdjClose',
                    'LAG30_AdjClose']
    spy_data = dh.get_etf_data(['SPY'], spy_features,
                               dt.datetime(2000, 1, 1),
                               dt.datetime(2019, 1, 1))
    dh.close_connections()
    spy_data['SpyRet1'] = spy_data.AdjClose / spy_data.LAG5_AdjClose
    spy_data['SpyRet2'] = spy_data.AdjClose / spy_data.LAG10_AdjClose
    spy_data['SpyRet3'] = spy_data.AdjClose / spy_data.LAG30_AdjClose
    spy_data = spy_data.drop(['SecCode', 'AdjClose', 'LAG5_AdjClose', 'LAG10_AdjClose', 'LAG30_AdjClose'], axis=1)
    spy_data['Date'] = spy_data.Date.apply(lambda z: z.to_pydatetime().date())
    return spy_data



if __name__ == '__main__':

    from ram.data.data_handler_sql import DataHandlerSQL
    from ram.analysis.run_manager import RunManager
    from sklearn.ensemble import RandomForestClassifier

    rm1 = RunManager('StatArbStrategy', 'run_0022', 2009)
    rm1.import_return_frame()

    spy_data = _get_some_spy_data()
    spy_features = spy_data.columns[1:].tolist()

    # Make features
    ret_features = make_features_from_rets(rm1.returns)
    features = ret_features.columns[2:].tolist()

    for sf in spy_features:
        features.append(sf)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    dates = []
    for y in range(2010, 2018):
        for m in range(1, 13):
            dates.append(dt.date(y, m, 1))
            dates.append(dt.date(y, m, 1) + dt.timedelta(days=14))

    top_params = 5

    ret_responses = make_responses(rm1.returns, 5)
    responses = ret_responses.columns[2:].tolist()
    out = pd.DataFrame(columns=responses)

    for d1, d2 in zip(dates[12:-1], dates[13:]):

        # Make responses and features
        ret_responses = make_responses(rm1.returns[rm1.returns.index < d1], 5)
        responses = ret_responses.columns[2:].tolist()

        train = ret_responses.merge(ret_features).dropna()
        train = train.merge(spy_data)

        if d1 > rm1.returns.index.max():
            break

        cl = RandomForestClassifier(min_samples_split=100, n_jobs=3)
        cl.fit(X=train[features], y=train[responses].astype(int))

        fit_date = rm1.returns.index[rm1.returns.index < d1].max()

        # Get best indexes by response variable
        predictions_data = ret_features[ret_features.Date == fit_date]
        predictions = cl.predict_proba(predictions_data[features])
        predictions = [x[:, 1] for x in predictions]

        temp = pd.DataFrame()

        for resp, preds in zip(responses, predictions):
            # Top 5
            top_inds = predictions_data.ColumnName.iloc[np.argsort(preds)[-top_params:]].values
            last_date = d2 - dt.timedelta(days=1)
            top_rets = pd.DataFrame(rm1.returns.loc[d1:last_date, top_inds].mean(axis=1), columns=[resp])
            temp = temp.join(top_rets, how='outer')

        out = out.append(temp)

        print d1


    #path = os.path.join(os.getenv('DATA'), 'ram', 'bigsimulation.csv')

    out.to_csv(path)

    #plot_inds = rm1.returns.loc[out.index.min():]
    #plt.figure()
    #plt.plot(plot_inds.cumsum(), '#999999', alpha=0.05)
    #plt.plot(out.cumsum(), 'b')
    #plt.show()


