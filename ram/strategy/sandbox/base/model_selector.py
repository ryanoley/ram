import os
import itertools
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt
from sklearn.ensemble import RandomForestRegressor

from ram.strategy.sandbox.base.features import create_model_perf_df, \
            add_model_response, get_model_param_binaries, get_market_features
from ram.analysis.run_manager import RunManager
from gearbox import create_time_index



class ModelSelector(object):

    sector_etf_map = {'Sector10':'iye', 'Sector15':'iym', 'Sector20':'iyj',
                      'Sector25':'iyk', 'Sector30':'iyk', 'Sector35':'iyh',
                      'Sector40':'iyf', 'Sector45':'qqq', 'Sector50':'iyz',
                      'Sector55':'idu'}

    def __init__(self, run_manager):
        assert isinstance(run_manager, RunManager)

        if not hasattr(run_manager, 'returns'):
            run_manager.import_return_frame()
        if not hasattr(run_manager, 'column_params'):
            run_manager.import_column_params()

        returns = run_manager.returns.copy()
        returns.index = [x.date() for x in returns.index]
        self.model_returns = returns

        q_index = create_time_index(returns.index, 'Q')
        m_index = create_time_index(returns.index, 'M')
        self.dt_index = pd.DataFrame({'Date':returns.index,
                                      'QIndex':q_index,
                                      'MIndex':m_index})

        self._set_strategy_features()
        self.model_params = self.prep_model_param_data(run_manager)
        self.run_manager = run_manager

    def prep_model_param_data(self, run_mgr):
        param_df = pd.DataFrame(run_mgr.column_params).transpose()
        param_df.reset_index(inplace=True)
        param_df.rename(columns={'index':'Model'}, inplace=True)
        return param_df

    def process_data(self, mkt_data_path, etf_tickers=None):

        data = create_model_perf_df(self.model_returns)
        data = self.filter_entry_dates(data)

        data = data.merge(self.dt_index, how='left')
        data = data.merge(self.model_params)
        data = add_model_response(data, self.model_returns)

        data = get_model_param_binaries(data, feature_cols=['binary_feature',
                                                            'sort_feature'])
        param_cols = [x for x in data.columns if x.find('attr_') == 0]
        self._set_strategy_features(self.features + param_cols)

        data.dropna(subset=self.features, inplace=True)
        data.sort_values(by=['Date', 'Model'], inplace=True)

        market_data = get_market_features(mkt_data_path, etf_tickers)
        self._set_etf_features(etfs=etf_tickers)
        market_data.dropna(subset=self.mkt_features, inplace=True)
        data = data.merge(market_data)

        data.reset_index(drop=True, inplace=True)
        self.processed_model_data = data

    def filter_entry_dates(self, data):
        holding_pers = self.model_params.rebalance_per.unique()
        training_dates = []

        for hp in holding_pers:
            train_dates = self.get_training_dates(hp)
            training_dates.extend(train_dates)

        data = data[data.Date.isin(training_dates)]
        return data.reset_index(drop=True)

    ############### ITERATOR ################

    def selection_iter(self, rebalance_per, selection_args):
        iter_models = self.model_params.copy()
        iter_models = iter_models[iter_models.rebalance_per == rebalance_per]
        iter_models = iter_models.Model.values

        all_data = self.get_processed_model_data()
        iter_data = all_data[all_data.Model.isin(iter_models)]

        returns, model_ix = self.get_per_returns(iter_data, **selection_args)
        stats = self.get_trade_details(model_ix)

        return returns, stats

    def get_per_returns(self, iter_data, n_models=30, t_start=24,
                        resp_days=30, active_models=False, n_estimators=50,
                        min_samples_leaf=500, n_jobs=-1):

        model_features = self.features + self.mkt_features
        response_var = 'resp_{}'.format(resp_days)
        assert response_var in iter_data.columns

        RFR = RandomForestRegressor(n_estimators = n_estimators,
                                    min_samples_leaf = min_samples_leaf,
                                    max_features = .8,
                                    n_jobs = n_jobs)
        RFR.random_state = 123

        all_returns = pd.DataFrame(index = self.dt_index.Date)
        all_returns['Ret'] = 0.
        tix_models = {}

        for t in tqdm(iter_data.MIndex.unique()[t_start:]):
            test = iter_data[iter_data.MIndex == t].copy()
            min_dt = test.Date.min()
            max_dt = self.dt_index[self.dt_index.MIndex == t].Date.max()

            train = iter_data[iter_data.MIndex < t].copy()
            train = self.trim_training_data(train, min_dt, resp_days)
            train.dropna(subset=[response_var], inplace=True)

            if active_models:
                active_model_ix = self.get_active_models(t)
                test = test[test.Model.isin(active_model_ix)]

            RFR.fit(X=train[model_features], y=train[response_var])
            test['pred'] = RFR.predict(X=test[model_features])
            test = test[test.Date == test.Date.min()]
            test.sort_values('pred', inplace=True)

            model_ix = test.Model[-n_models:].values
            tix_returns = self.model_returns.loc[min_dt:max_dt,
                                                    model_ix].mean(axis=1)
            all_returns.loc[tix_returns.index, 'Ret'] = tix_returns
            tix_models[t] = model_ix

        return all_returns, tix_models

    def get_trade_details(self, model_ix, max_pos = .1, port_size=5e6):
        # Model ix dict keyed by TIndex with the selected models for that qtr

        if not hasattr(self, 'all_output'):
            self.run_manager.import_all_output()
            all_output = self.run_manager.all_output.copy()
            verify_position_counts(all_output)
            all_output.index = [x.date() for x in all_output.index]
            self.all_output = all_output

        detail_frame = pd.DataFrame([])

        for t_ix, t_models in model_ix.iteritems():

            tix_dates = self.dt_index[self.dt_index.MIndex == t_ix].Date.values
            tix_detail = pd.DataFrame(index = tix_dates)

            pos_cols = ['OpenLongPositions_{}'.format(i) for i in t_models]
            gross_cols = ['GrossExposure_{}'.format(i) for i in t_models]
            net_cols = ['NetExposure_{}'.format(i) for i in t_models]

            tix_positions = self.all_output.loc[tix_dates, pos_cols].values
            tix_gross_exp = self.all_output.loc[tix_dates, gross_cols].values
            tix_net_exp = self.all_output.loc[tix_dates, net_cols].values

            tix_pos_size = 1 / (tix_positions * 2) # 2x for shorts
            tix_pos_size[tix_pos_size == np.inf] = 0.
            tix_pos_size[tix_pos_size > max_pos] = max_pos

            tix_detail['position_size'] = tix_pos_size.mean(axis=1)
            tix_detail['gross_exp'] = tix_gross_exp.mean(axis=1) / port_size
            tix_detail['net_exp'] = tix_net_exp.mean(axis=1) / port_size

            if len(detail_frame) == 0:
                detail_frame = tix_detail
            else:
                detail_frame = detail_frame.append(tix_detail)

        return detail_frame.sort_index()

    ################ HELPERS #################

    def get_processed_model_data(self):
        if not hasattr(self, 'processed_model_data'):
            return
        else:
            return self.processed_model_data.copy()

    def get_training_dates(self, rebal_per, research_per='Q'):
        train_dts = []

        if research_per == 'M':
            t_indices = self.dt_index.MIndex.unique()
            t_col = 'MIndex'
        else:
            t_indices = self.dt_index.QIndex.unique()
            t_col = 'QIndex'

        for t in t_indices:
            tix_dt_index = self.dt_index[self.dt_index[t_col] == t]
            tix_train_dts = tix_dt_index.Date.values
            tix_train_dts = tix_train_dts[::rebal_per]
            train_dts.extend(tix_train_dts)

        return train_dts

    def get_active_models(self, t_ix):
        # This function approximates models that are trading in a time per
        # by returning models with any p&l in that period
        t_dates = self.dt_index[self.dt_index.MIndex == t_ix].Date.values
        t_returns = self.model_returns.loc[t_dates]
        trading_flag = np.any(t_returns != 0, axis=0)
        trading_models = self.model_returns.columns[trading_flag]

        return trading_models

    def trim_training_data(self, train_data, max_dt, n_days):
        # This function returns trian_data cut_off to n_days business days
        # before the max_dt
        train_dts = self.dt_index[self.dt_index.Date <= max_dt]
        train_dts = train_dts.sort_values(by='Date')
        max_train_dt = train_dts.Date.iloc[-n_days]
        trimmed_data = train_data[train_data.Date <= max_train_dt]

        return trimmed_data.reset_index(drop=True)

    def _set_strategy_features(self, strategy_features=None):
        features = [
            'cum_ret', 'ms10', 'ms20', 'ms60', 'ms120', 'ms250', 'ms500',
            'std60', 'std120', 'std250', 'std500', 'crma60', 'crma120',
            'crma250'
        ]

        if strategy_features is None:
            self.features = features
        else:
            self.features = strategy_features

        return

    def _set_etf_features(self, features=None, etfs=None):

        if features is None:
            features = [
                'roll_ret_10', 'roll_ret_20', 'roll_ret_60', 'roll_ret_120',
                'roll_ret_250', 'roll_ret_500', 'DISCOUNT126_AdjClose',
                'DISCOUNT252_AdjClose', 'DISCOUNT500_AdjClose', 'VOL20_AdjClose',
                'VOL50_AdjClose', 'VOL100_AdjClose', 'VOL250_AdjClose'
                ]

        if etfs is None:
            etfs = ['spy', 'iwm', 'iye', 'iym', 'iyj', 'iyk', 'iyh', 'iyf',
                    'qqq', 'iyz', 'idu']

        mkt_features = ['{}_{}'.format(x,y) for (x,y) in
                            itertools.product(etfs, features)]

        self.mkt_features = mkt_features

        return

def verify_position_counts(output_df):
    # Long and short positions should be equal for each model by design
    long_cols = [c for c in output_df.columns if c.find('OpenLong') == 0]
    short_cols = [c for c in output_df.columns if c.find('OpenShort') == 0]
    long_cols.sort()
    short_cols.sort()
    mismatch = (output_df[long_cols].values != output_df[short_cols].values)
    assert mismatch.sum() == 0
    return

if __name__ == '__main__':
    import ipdb; ipdb.set_trace()

    ###  SINGLE SECTOR
    mkt_data_dir = os.path.join(os.getenv('DATA'), 'ram', 'prepped_data',
                                'sirank', 'etfs')
    sim_dir = os.path.join(os.getenv('DATA'), 'ram', 'simulations')

    rm = RunManager('Sandbox', 'run_0057', 2007, test_periods=0)
    ms = ModelSelector(rm)
    ms.process_data(mkt_data_dir,
                    etf_tickers=['spy','iwm', ms.sector_etf_map['Sector15']])


    selection_args = {'n_models':30, 'resp_days':20, 't_start':36,
                                'active_models':True, 'n_estimators':25,
                                'min_samples_leaf':1000, 'n_jobs':2}
    returns, stats = ms.selection_iter(rebalance_per=8,
                                         selection_args=selection_args)



    ##################################
    #### LARGE RUN ALL SECTORS AWS####
    ##################################
    sim_dir = '/ramdata/src/ram/simulations'
    mkt_data_dir = '/ramdata/src/ram/prepped_data/Sandbox/etfs'

    runs = {'Sector10':'run_0056',
            'Sector15':'run_0057',
            'Sector20':'run_0058',
            'Sector25':'run_0059',
            'Sector30':'run_0060',
            'Sector35':'run_0061',
            'Sector40':'run_0062',
            'Sector45':'run_0063',
            'Sector50':'run_0064',
            'Sector55':'run_0065'}

    selection_args = {'n_models':20, 'resp_days':20, 't_start':36,
                      'active_models':True, 'n_estimators':90,
                      'min_samples_leaf':500, 'n_jobs':-1}

    sector_rets = pd.DataFrame([])
    sector_stats = pd.DataFrame([])

    for sec, run_name in runs.iteritems():
        rm = RunManager('Sandbox', run_name, 2007, test_periods=0,
                         simulation_data_path=sim_dir)
        ms = ModelSelector(rm)
        ms.process_data(mkt_data_dir,
                        etf_tickers=['spy','iwm', ms.sector_etf_map[sec]])

        returns8, stats8 = ms.selection_iter(rebalance_per=8,
                                               selection_args=selection_args)
        returns8.rename(columns={'Ret':'{}_Ret8'.format(sec)}, inplace=True)
        stats8.columns = ['{}_{}_per8'.format(c, sec) for c in stats8.columns]

        sector_rets = sector_rets.merge(returns8, left_index=True,
                                        right_index=True, how='outer')

        sector_stats = sector_stats.merge(stats8, left_index=True,
                                        right_index=True, how='outer')

        sector_rets.to_csv('/ramdata/src/ram/simulations/n20_r20_Act_rets.csv')
        sector_stats.to_csv('/ramdata/src/ram/simulations/n20_r20_Act_stats.csv')
