import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.data.data_handler_sql import DataHandlerSQL
from ram.analysis.run_manager import RunManager

from sklearn.ensemble import RandomForestClassifier

# View all available strategies
print RunManager.get_strategies()
print RunManager.get_run_names('StatArbStrategy')

rm1 = RunManager('StatArbStrategy', 'run_0021', 2009)

rm1.import_return_frame()


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


def make_responses(return_data):
    out = _unpivot(_get_ranks(return_data, 5, 10), 'Response5_10')
    out = out.merge(_unpivot(_get_ranks(return_data, 5, 20), 'Response5_20'))
    out = out.merge(_unpivot(_get_ranks(return_data, 5, 30), 'Response5_30'))

    out = out.merge(_unpivot(_get_ranks(return_data, 10, 10), 'Response10_10'))
    out = out.merge(_unpivot(_get_ranks(return_data, 10, 20), 'Response10_20'))
    out = out.merge(_unpivot(_get_ranks(return_data, 10, 30), 'Response10_30'))
    out = out.merge(_unpivot(_get_ranks(return_data, 10, 40), 'Response10_40'))
    out = out.merge(_unpivot(_get_ranks(return_data, 10, 50), 'Response10_50'))

    out = out.merge(_unpivot(_get_ranks(return_data, 20, 10), 'Response20_10'))
    out = out.merge(_unpivot(_get_ranks(return_data, 20, 20), 'Response20_20'))
    out = out.merge(_unpivot(_get_ranks(return_data, 20, 30), 'Response20_30'))
    out = out.merge(_unpivot(_get_ranks(return_data, 20, 40), 'Response20_40'))
    out = out.merge(_unpivot(_get_ranks(return_data, 20, 50), 'Response20_50'))
    return out

ret_responses = make_responses(rm1.returns)



# ~~~~~~ Features ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def make_features_from_rets(return_data):
    out = _unpivot(return_data.rolling(20).mean() / return_data.rolling(20).std(), 'Sharpe10')
    out = out.merge(_unpivot(return_data.rolling(20).mean() / return_data.rolling(20).std(), 'Sharpe20'))
    out = out.merge(_unpivot(return_data.rolling(60).mean() / return_data.rolling(60).std(), 'Sharpe60'))
    out = out.merge(_unpivot(return_data.rolling(20).min(), 'Min20'))
    out = out.merge(_unpivot(return_data.rolling(60).min(), 'Min60'))
    out = out.merge(_unpivot(return_data.rolling(5).mean(), 'Mean5'))
    out = out.merge(_unpivot(return_data.rolling(10).mean(), 'Mean10'))
    out = out.merge(_unpivot(return_data.rolling(20).mean(), 'Mean20'))
    return out

ret_features = make_features_from_rets(rm1.returns)
features = ret_features.columns[2:].tolist()
responses = ret_responses.columns[2:].tolist()

aligned_data = ret_responses.merge(ret_features).dropna()




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dh = DataHandlerSQL()

spy_features = ['PRMA10_AdjClose', 'PRMA30_AdjClose', 'VOL10_AdjClose',
                'VOL30_AdjClose', 'DISCOUNT50_AdjClose', 'RSI10_AdjClose',
                'AdjClose', 'LAG5_AdjClose', 'LAG10_AdjClose', 'LAG30_AdjClose']
spy_data = dh.get_etf_data(['SPY'], spy_features, dt.datetime(2000, 1, 1), dt.datetime(2019, 1, 1))

dh.close_connections()

spy_data['SpyRet1'] = spy_data.AdjClose / spy_data.LAG5_AdjClose
spy_data['SpyRet2'] = spy_data.AdjClose / spy_data.LAG10_AdjClose
spy_data['SpyRet3'] = spy_data.AdjClose / spy_data.LAG30_AdjClose

spy_data = spy_data.drop(['SecCode', 'AdjClose', 'LAG5_AdjClose', 'LAG10_AdjClose', 'LAG30_AdjClose'], axis=1)
spy_data['Date'] = spy_data.Date.apply(lambda z: z.to_pydatetime().date())

spy_features = spy_data.columns[1:].tolist()

aligned_data = aligned_data.merge(spy_data)
for sf in spy_features:
    features.append(sf)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


dates = []
for y in range(2010, 2018):
    for m in range(1, 13):
        dates.append(dt.date(y, m, 1))
        dates.append(dt.date(y, m, 1) + dt.timedelta(days=14))

top_params = 5

out = pd.DataFrame(columns=responses)

for d1, d2 in zip(dates[12:-1], dates[13:]):

    train = aligned_data[aligned_data.Date < d1]
    test = aligned_data[(aligned_data.Date >= d1) & (aligned_data.Date < d2)]

    if len(test) == 0:
        break

    cl = RandomForestClassifier(min_samples_split=400, n_jobs=3)
    cl.fit(X=train[features], y=train[responses].astype(int))

    fit_date = train.Date.max()

    # Get best indexes by response variable
    predictions_data = train[train.Date == fit_date]
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


path = os.path.join(os.getenv('DATA'), 'ram', 'bigsimulation.csv')

out.to_csv(path)


#plot_inds = rm1.returns.loc[out.index.min():]
#plt.figure()
#plt.plot(plot_inds.cumsum(), '#999999', alpha=0.05)
#plt.plot(out.cumsum(), 'b')
#plt.show()


