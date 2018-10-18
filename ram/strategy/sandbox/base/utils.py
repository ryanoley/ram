import numpy as np
import pandas as pd
import datetime as dt


def make_variable_dict(data, variable, fillna=np.nan):
    data_pivot = data.pivot(index='Date', columns='SecCode', values=variable)
    if fillna == 'pad':
        data_pivot = data_pivot.fillna(method='pad')
    else:
        data_pivot = data_pivot.fillna(fillna)
    return data_pivot.T.to_dict()

def pull_hedge_data(output_path, tickers=['SPY']):
    from ram.data.data_handler_sql import DataHandlerSQL
    dh = DataHandlerSQL()
    start_date = dt.datetime(1999,1,1,0,0)
    end_date = dt.datetime(2020,1,1,0,0)
    features = ['SplitFactor', 'RVwap', 'RClose', 'RCashDividend',
                'AdjClose', 'AdjOpen', 'AdjVwap', 'LEAD1_AdjVwap',
                'LEAD4_AdjVwap', 'LEAD5_AdjVwap',  'LEAD6_AdjVwap',
                'LEAD7_AdjVwap', 'LEAD8_AdjVwap',  'LEAD9_AdjVwap',
                'LEAD10_AdjVwap', 'LEAD11_AdjVwap', 'LEAD16_AdjVwap',

                'PRMA10_AdjClose', 'PRMA20_AdjClose', 'PRMA60_AdjClose',
                'PRMA120_AdjClose', 'PRMA250_AdjClose',

                'DISCOUNT126_AdjClose','DISCOUNT252_AdjClose',
                'DISCOUNT500_AdjClose',

                'VOL20_AdjClose', 'VOL50_AdjClose', 'VOL100_AdjClose',
                'VOL250_AdjClose'
                ]
    spy_data = dh.get_etf_data(tickers, features, start_date, end_date)
    spy_data.to_csv(output_path, index=False)
    return


#########################################
########### MODEL SELECTION #############
#########################################

def append_col(data, var_pivot, col_name):
    assert(set(['Model', 'Date']).issubset(set(data.columns)))
    var_pivot = var_pivot.unstack().reset_index()
    var_pivot.columns = ['Model', 'Date', col_name]
    data = data.merge(var_pivot, how='left')
    return data

def summarize_run(run_manager, top_n, bottom_n):
    if not hasattr(run_manager, 'returns'):
        run_manager.import_return_frame()
    if not hasattr(run_manager, 'column_params'):
        run_manager.import_column_params()

    return_frame = run_manager.returns.copy()
    ret_ix = np.argsort(return_frame.sum())
    param_dict = run_manager.column_params
    top_df = pd.DataFrame(columns = param_dict['1'].keys())
    btm_df = pd.DataFrame(columns = param_dict['1'].keys())

    top_ix = ret_ix[-top_n:]
    btm_ix = ret_ix[:bottom_n]

    top_returns = return_frame[top_ix].sum()
    btm_returns = return_frame[btm_ix].sum()

    top_params = top_returns[top_returns > 0].index.values
    btm_params = btm_returns[btm_returns < 0].index.values

    for i, tp in enumerate(top_params):
        top_df.loc[i] = param_dict[tp]

    for i, bp in enumerate(btm_params):
        btm_df.loc[i] = param_dict[bp]

    out_top = pd.DataFrame([])
    out_btm = pd.DataFrame([])
    for col in top_df.columns:
        col_df = top_df[col].value_counts().to_frame()
        col_df.columns = ['count']
        col_df['value'] = col_df.index.copy()
        col_df.reset_index(drop=True, inplace=True)
        col_df['param'] = col
        out_top = out_top.append(col_df)
        out_top = out_top[['param', 'value', 'count']]
        out_top.sort_values(['param', 'count'], inplace=True)

        col_df = btm_df[col].value_counts().to_frame()
        col_df.columns = ['count']
        col_df['value'] = col_df.index.copy()
        col_df.reset_index(drop=True, inplace=True)
        col_df['param'] = col
        out_btm = out_btm.append(col_df)
        out_btm = out_btm[['param', 'value', 'count']]
        out_btm.sort_values(['param', 'count'], inplace=True)
    return out_top, out_btm


