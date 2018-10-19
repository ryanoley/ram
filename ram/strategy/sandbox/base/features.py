import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.sandbox.base.utils import append_col
from gearbox import convert_date_array


def create_split_multiplier(data):
    split = data.pivot(index='Date', columns='SecCode',
                       values='SplitFactor').pct_change().fillna(0) + 1
    split = split.unstack().reset_index()
    split.columns = ['SecCode', 'Date', 'SplitMultiplier']
    return data.merge(split).drop('SplitFactor', axis=1)

def n_day_high_low(data, column, n_day, out_col_name, low=False):

    values = data.pivot(index='Date', columns='SecCode',
                       values=column)
    if low:
        high_low = values.rolling(window=n_day, min_periods=0).min()
    else:
        high_low = values.rolling(window=n_day, min_periods=0).max()

    flag = values.copy()
    flag[:] = 0
    flag[values == high_low] = 1
    flag = flag.unstack().reset_index()
    flag.columns = ['SecCode', 'Date', out_col_name]
    return data.merge(flag)

def n_pct_top_btm(data, col_name, pct_int, out_col_name, btm_pct=False):

    values = data[col_name].dropna().values
    pct_val = 100 - pct_int if not btm_pct else pct_int
    pct_val = np.percentile(values, pct_val)

    return_df = data.copy()
    if not btm_pct:
        return_df[out_col_name] = return_df[col_name] >= pct_val
    else:
        return_df[out_col_name] = return_df[col_name] <= pct_val

    return_df[out_col_name].fillna(False, inplace=True)
    return_df[out_col_name] = return_df[out_col_name].astype(int)

    return return_df

def ewma_rsi(data, window):
    assert('AdjClose' in data.columns)
    pdata = data[['AdjClose']].copy()
    pdata['px_change'] = pdata.AdjClose - pdata.AdjClose.shift(1)
    pdata['up_move'] = np.where(pdata.px_change>0, pdata.px_change, 0)
    pdata['down_move'] = np.where(pdata.px_change<0, -pdata.px_change, 0)
    pdata['ewma_up'] = pdata.up_move.ewm(span=window).mean()
    pdata['ewma_down'] = pdata.down_move.ewm(span=window).mean()
    pdata['RS'] = pdata.ewma_up / pdata.ewma_down
    pdata['RSI'] = (100 - (100 / (1+pdata.RS)))
    return pdata.RSI.values

############# RESPONSES ##################

def get_vwap_returns(data, days, hedged=False, market_data=None):
    exit_col = 'LEAD{}_AdjVwap'.format(days +  1)
    ret_col = 'Ret{}'.format(days)

    assert set(['LEAD1_AdjVwap', exit_col]).issubset(data.columns)
    prices  = data[['SecCode', 'Date', exit_col, 'LEAD1_AdjVwap']].copy()
    prices[ret_col] = (prices[exit_col] / prices.LEAD1_AdjVwap)

    if hedged:
        assert set(['LEAD1_AdjVwap', exit_col]).issubset(market_data.columns)
        spy_prices  = market_data[['Date', exit_col, 'LEAD1_AdjVwap']].copy()
        spy_prices['MktRet'] = (spy_prices[exit_col] / spy_prices.LEAD1_AdjVwap)
        prices = prices.merge(spy_prices[['Date', 'MktRet']], how='left')
        prices[ret_col] -= prices.MktRet
    else:
        prices[ret_col] -= 1

    data = data.merge(prices[['SecCode', 'Date', ret_col]], how='left')
    return data

def two_var_signal(pivot_binary_var, pivot_sort_var, sort_pct):
    '''
    Return a pivot table same shape as binary_var and sort_var with signals
    1 and -1.  These represent high and low groups of size sort_pct of
    universe with binary flag and then sorted by sort_var.
    '''
    univ_sizes = (pivot_binary_var.sum(axis=1) * sort_pct).astype(int)

    sort_arr = pivot_sort_var.values
    binary_arr = pivot_binary_var.values.astype(bool)
    na_filter = ~np.isnan(sort_arr)

    out_signals = pivot_sort_var.copy()
    out_signals[:] = 0
    seccodes = out_signals.columns.values

    for i in range(sort_arr.shape[0]):
        n_take = univ_sizes.iloc[i]
        if n_take == 0:
            continue
        nans = na_filter[i, :]
        s_vals = sort_arr[i, :][nans]
        b_vals = binary_arr[i, :][nans]

        ix = np.argsort(s_vals)
        low_codes = seccodes[nans][ix][b_vals][:n_take]
        high_codes = seccodes[nans][ix][b_vals][-n_take:]

        out_signals.iloc[i, :][low_codes] = -1
        out_signals.iloc[i, :][high_codes] = 1

    out_df = out_signals.unstack().reset_index()
    out_df.columns = ['SecCode', 'Date', 'signal']
    return out_df


#########################################
########### MODEL SELECTION #############
#########################################

def get_etf_features(etf_ticker, data_dir):
    # Build data frame for a single etf
    etf_data_path = os.path.join(data_dir, '{}.csv'.format(etf_ticker))
    assert(os.path.exists(etf_data_path))

    etf_data = pd.read_csv(etf_data_path)
    etf_data['Ret'] = etf_data.AdjClose.pct_change(1)
    etf_data['roll_ret_10'] = etf_data.rolling(10, min_periods=1).Ret.sum()
    etf_data['roll_ret_20'] = etf_data.rolling(20, min_periods=1).Ret.sum()
    etf_data['roll_ret_60'] = etf_data.rolling(60, min_periods=1).Ret.sum()
    etf_data['roll_ret_120'] = etf_data.rolling(120, min_periods=1).Ret.sum()
    etf_data['roll_ret_250'] = etf_data.rolling(250, min_periods=1).Ret.sum()
    etf_data['roll_ret_500'] = etf_data.rolling(500, min_periods=1).Ret.sum()

    etf_columns = ['Date', 'Ret', 'roll_ret_10','roll_ret_20', 'roll_ret_60',
                    'roll_ret_120', 'roll_ret_250', 'roll_ret_500',
                    'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',
                    'DISCOUNT500_AdjClose', 'VOL20_AdjClose', 'VOL50_AdjClose',
                    'VOL100_AdjClose', 'VOL250_AdjClose']

    etf_data = etf_data[etf_columns]
    etf_data.columns = ['Date'] + ['{}_{}'.format(etf_ticker, c) for c
                                    in etf_columns[1:]]
    return etf_data

def get_market_features(mkt_data_path=os.path.join(os.getenv('DATA'), 'ram',
                                                   'prepped_data','sirank'),
                        etf_tickers=None):

    if etf_tickers is None:
        fls = os.listdir(mkt_data_path)
        etf_tickers = [fl.replace('.csv', '') for fl in fls if fl.find('csv') > 0]

    market_data = pd.DataFrame(columns=['Date'])
    for etf in etf_tickers:
        etf_data = get_etf_features(etf, mkt_data_path)
        market_data = market_data.merge(etf_data, how='outer')

    market_data.Date = convert_date_array(market_data.Date)
    return market_data

def create_model_perf_df(returns):

    cum_ret = returns.cumsum()
    data = cum_ret.unstack().reset_index()
    data.columns = ['Model', 'Date', 'cum_ret']

    ms10 = returns.rolling(window=10,
                           min_periods=1).sum().fillna(method='pad')
    ms20 = returns.rolling(window=20,
                           min_periods=1).sum().fillna(method='pad')
    ms60 = returns.rolling(window=60,
                           min_periods=1).sum().fillna(method='pad')
    ms120 = returns.rolling(window=120,
                            min_periods=1).sum().fillna(method='pad')
    ms250 = returns.rolling(window=250,
                            min_periods=1).sum().fillna(method='pad')
    ms500 = returns.rolling(window=500,
                            min_periods=1).sum().fillna(method='pad')

    std60 = returns.rolling(window=60,
                            min_periods=1).std().fillna(method='pad')
    std120 = returns.rolling(window=120,
                             min_periods=1).std().fillna(method='pad')
    std250 = returns.rolling(window=250,
                             min_periods=1).std().fillna(method='pad')
    std500 = returns.rolling(window=500,
                             min_periods=1).std().fillna(method='pad')

    ma20 = cum_ret.rolling(window=20,
                           min_periods=1).mean().fillna(method='pad')
    ma60 = cum_ret.rolling(window=60,
                           min_periods=1).mean().fillna(method='pad')
    ma120 = cum_ret.rolling(window=120,
                            min_periods=1).mean().fillna(method='pad')
    ma250 = cum_ret.rolling(window=250,
                            min_periods=1).mean().fillna(method='pad')
    crma60 = cum_ret / ma60
    crma120 = cum_ret / ma120
    crma250 = cum_ret / ma250

    data = append_col(data, ms10, 'ms10')
    data = append_col(data, ms20, 'ms20')
    data = append_col(data, ms60, 'ms60')
    data = append_col(data, ms120, 'ms120')
    data = append_col(data, ms250, 'ms250')
    data = append_col(data, ms500, 'ms500')

    data = append_col(data, std60, 'std60')
    data = append_col(data, std120, 'std120')
    data = append_col(data, std250, 'std250')
    data = append_col(data, std500, 'std500')

    data = append_col(data, crma60, 'crma60')
    data = append_col(data, crma120, 'crma120')
    data = append_col(data, crma250, 'crma250')

    return data

def add_model_response(data, returns):
    resp_10 = returns.rolling(10).sum().shift(-10)
    resp_20 = returns.rolling(20).sum().shift(-20)
    resp_30 = returns.rolling(30).sum().shift(-30)
    resp_40 = returns.rolling(40).sum().shift(-40)

    data = append_col(data, resp_10, 'resp_10')
    data = append_col(data, resp_20, 'resp_20')
    data = append_col(data, resp_30, 'resp_30')
    data = append_col(data, resp_40, 'resp_40')

    return data

def get_model_param_binaries(inp_data,
                             feature_cols=['binary_feature','sort_feature',
                                            'sort_pct','training_qtrs']):
    assert set(feature_cols).issubset(inp_data.columns)

    for fc in feature_cols:
        for val in inp_data[fc].unique():
            inp_data['_'.join(['attr', fc, str(val)])] = inp_data[fc] == val
    return inp_data

