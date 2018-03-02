import os
import json
import pickle
import numpy as np
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.statarb2 import prod_config
from ram.strategy.statarb2.main import StatArbStrategy2

from gearbox import convert_date_array

from ramex.orders.orders import MOCOrder
from ramex.client.client import ExecutionClient


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_raw_data(implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    """
    Loads implementation raw data and processes it
    """
    statarb_path = os.path.join(implementation_dir, 'StatArbStrategy2',
                                'daily_raw_data')
    all_files = _get_all_raw_data_file_names(statarb_path)
    todays_files = _get_max_date_files(all_files)
    output = {}
    print('Importing raw_data...')
    for f in todays_files:
        name = _format_raw_data_name(f)
        output[name] = _import_format_raw_data(os.path.join(statarb_path, f))
    output['market_data'] = _import_format_raw_data(
        os.path.join(statarb_path, 'market_index_data.csv'))
    return output


def _get_all_raw_data_file_names(raw_data_dir_path):
    """
    Filters out market_index_data.csv
    """
    all_files = os.listdir(raw_data_dir_path)
    all_files = [x for x in all_files if x.find('current_blueprint') > 0]
    all_files.sort()
    return all_files


def _get_max_date_files(all_files):
    max_date = max([x.split('_')[0] for x in all_files])
    todays_files = [x for x in all_files if x.find(max_date) > -1]
    todays_files = [x for x in todays_files if x.find('.csv') > -1]
    return todays_files


def _format_raw_data_name(file_name):
    return file_name[file_name.rfind('version'):].replace('.csv', '')


def _import_format_raw_data(path):
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_live_pricing(implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    # SCALING DATA
    scaling_data = _import_scaling_data(implementation_dir)
    bloomberg_data = _import_bloomberg_data(implementation_dir)
    scaling = scaling_data.merge(bloomberg_data, how='left').fillna(1)
    scaling['PricingMultiplier'] = \
        scaling.QADirectDividendFactor * \
        scaling.BbrgDivMultiplier * \
        scaling.BbrgSpinoffMultiplier * \
        scaling.BbrgSplitMultiplier
    scaling['VolumeMultiplier'] = scaling.BbrgSplitMultiplier
    scaling = scaling[['SecCode', 'PricingMultiplier', 'VolumeMultiplier']]

    # Index scaling
    scaling_t = pd.DataFrame()
    scaling_t['SecCode'] = ['50311', '11113']
    scaling_t['PricingMultiplier'] = 1
    scaling_t['VolumeMultiplier'] = 1
    scaling = scaling.append(scaling_t).reset_index(drop=True)

    # IMPORT LIVE AND ADJUST
    live_data = _import_live_pricing(implementation_dir)
    data = live_data.merge(scaling)
    data['RClose'] = data.AdjClose
    data.AdjOpen = data.AdjOpen * data.PricingMultiplier
    data.AdjHigh = data.AdjHigh * data.PricingMultiplier
    data.AdjLow = data.AdjLow * data.PricingMultiplier
    data.AdjClose = data.AdjClose * data.PricingMultiplier
    data.AdjVwap = data.AdjVwap * data.PricingMultiplier
    data.AdjVolume = data.AdjVolume * data.VolumeMultiplier
    data = data.drop(['PricingMultiplier', 'VolumeMultiplier'], axis=1)
    return data


def _import_live_pricing(implementation_dir):
    dtypes = {'SecCode': str, 'Ticker': str, 'Issuer': str,
              'CLOSE': np.float64, 'LAST': np.float64, 'OPEN': np.float64,
              'HIGH': np.float64, 'LOW': np.float64, 'VWAP': np.float64,
              'VOLUME': np.float64}
    path = os.path.join(implementation_dir, 'StatArbStrategy2',
                        'live_pricing', 'prices.csv')
    live_data = pd.read_csv(path, na_values=['na'], dtype=dtypes)
    live_data['AdjOpen'] = live_data.OPEN
    live_data['AdjHigh'] = live_data.HIGH
    live_data['AdjLow'] = live_data.LOW
    live_data['AdjClose'] = live_data.LAST
    live_data['AdjVolume'] = live_data.VOLUME
    live_data['AdjVwap'] = live_data.VWAP
    live_data = live_data[['SecCode', 'Ticker', 'AdjOpen', 'AdjHigh',
                           'AdjLow', 'AdjClose', 'AdjVolume', 'AdjVwap']]
    return live_data


def _import_scaling_data(implementation_dir):
    path = os.path.join(implementation_dir, 'StatArbStrategy2',
                        'live_pricing', 'seccode_scaling.csv')
    scaling = pd.read_csv(path)
    scaling.SecCode = scaling.SecCode.astype(str)
    scaling.Date = convert_date_array(scaling.Date)
    scaling['QADirectDividendFactor'] = scaling.DividendFactor
    scaling = scaling[['SecCode', 'QADirectDividendFactor']]
    return scaling


def _import_bloomberg_data(implementation_dir):
    dpath = os.path.join(implementation_dir, 'StatArbStrategy2',
                         'live_pricing', 'bloomberg_scaling.csv')
    data = pd.read_csv(dpath)
    cols = np.array(['Ticker', 'DivMultiplier', 'SpinoffMultiplier',
                     'SplitMultiplier'])

    assert np.all(data.columns == cols)
    data.columns = ['Ticker', 'BbrgDivMultiplier', 'BbrgSpinoffMultiplier',
                    'BbrgSplitMultiplier']
    dpath = os.path.join(implementation_dir, 'StatArbStrategy2',
                         'live_pricing', 'ticker_mapping.csv')
    tickers = pd.read_csv(dpath)
    data = data.merge(tickers)
    data.SecCode = data.SecCode.astype(str)
    return data[['SecCode', 'BbrgDivMultiplier', 'BbrgSpinoffMultiplier',
                 'BbrgSplitMultiplier']]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_run_map(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR,
        trained_model_dir_name=prod_config.trained_models_dir_name):

    path = os.path.join(implementation_dir,
                        'StatArbStrategy2',
                        'trained_models',
                        trained_model_dir_name,
                        'run_map.csv')
    data = pd.read_csv(path)
    # GCP outputs index column, which for this needs to be removed
    if data.columns[0].find('Unnamed') > -1:
        data = data.iloc[:, 1:]
    # Sort by data version
    data = data.sort_values('data_version').reset_index()
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_models_params(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR,
        trained_model_dir_name=prod_config.trained_models_dir_name):
    """
    Returns
    -------
    output : dict
        Holds parameter and sklearn model for each trained model
    """
    path = os.path.join(implementation_dir,
                        'StatArbStrategy2',
                        'trained_models',
                        trained_model_dir_name)
    model_files, param_files = _get_model_files(path)
    output = {}
    print('Importing models and parameters...')
    for m, p in zip(model_files, param_files):
        run_name = m.replace('_skl_model.pkl', '')
        output[run_name] = {}
        output[run_name]['params'] = \
            json.load(open(os.path.join(path, p), 'r'))
        output[run_name]['model'] = \
            pickle.load(open(os.path.join(path, m), 'r'))
    return output


def _get_model_files(path):
    """
    Return file names from production trained models directories, and
    makes sure the model name is aligned with the param file name
    """
    all_files = os.listdir(path)
    all_files = [x for x in all_files if x.find('run_map.csv') == -1]
    model_files = [x for x in all_files if x.find('skl_model') > -1]
    model_files.sort()
    param_files = [x for x in all_files if x.find('params') > -1]
    param_files.sort()
    # Assert that they are aligned
    for m, p in zip(model_files, param_files):
        assert m.replace('_skl_model.pkl', '') == p.replace('_params.json', '')
    return model_files, param_files


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_portfolio_manager_positions(
        position_sheet_dir=config.POSITION_SHEET_DIR):
    all_files = os.listdir(position_sheet_dir)
    all_files = [x for x in all_files if x.find('positions.csv') > -1]
    file_name = max(all_files)
    positions = pd.read_csv(os.path.join(position_sheet_dir, file_name))
    positions = positions[['position', 'symbol', 'share_count']]
    # Locate positions with correct statarb prefix
    inds = [x.find('StatArb2_') > -1 for x in positions.position]
    positions = positions.loc[inds].reset_index(drop=True)
    return positions


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class StatArbImplementation(object):

    def __init__(self, StatArbStrategy=StatArbStrategy2):
        # Used for testing
        self.StatArbStrategy = StatArbStrategy2

    def add_raw_data(self, data):
        self.raw_data = data

    def add_run_map(self, run_map):
        self.run_map = run_map

    def add_models_params(self, models_params):
        self.models_params_strategy = models_params

    def add_positions(self, positions):
        self.positions = positions

    def prep(self):
        assert hasattr(self, 'models_params_strategy')
        assert hasattr(self, 'run_map')
        assert hasattr(self, 'raw_data')
        print('Prepping data...')
        for i, vals in self.run_map.iterrows():
            strategy = self.StatArbStrategy(
                strategy_code_version=vals.strategy_version
            )
            strategy.strategy_init()
            strategy.data.prep_live_data(
                data=self.raw_data[vals.data_version],
                market_data=self.raw_data['market_data']
            )
            self.models_params_strategy[vals.param_name]['strategy'] = \
                strategy
        print('Finished prepping data...')
        return

    def start(self):
        pass

    def run_live(self, live_data):

        orders = pd.DataFrame()

        for name, objs in self.models_params_strategy.iteritems():
            strategy = objs['strategy']
            params = objs['params']
            model = objs['model']

            strategy.data.process_live_data(live_data)

            sparams = _extract_params(params, strategy.signals.get_args())
            strategy.signals.set_args(**sparams)

            strategy.signals.set_features(strategy.data.get_train_features())
            strategy.signals.set_test_data(strategy.data.get_test_data())
            strategy.signals.set_model(model)

            signals = strategy.signals.get_signals()

            cparams = _extract_params(params, strategy.constructor.get_args())
            strategy.constructor.set_args(**cparams)
            strategy.constructor.set_signals_constructor_data(
                signals, strategy.data.get_constructor_data())

            scores = signals[['SecCode', 'preds']].set_index(
                'SecCode').to_dict()['preds']

            sizes = strategy.constructor .get_day_position_sizes(0, scores)
            sizes = pd.Series(sizes).reset_index()
            sizes.columns = ['SecCode', 'Dollars']
            sizes['Strategy'] = name
            orders = orders.append(sizes)

        return orders


def _extract_params(all_params, selected_params):
    out = {}
    for key in selected_params.keys():
        out[key] = all_params[key]
    return out


def _add_sizes(all_sizes, model_sizes):
    for key, val in model_sizes.iteritems():
        if key not in all_sizes:
            all_sizes[key] = 0
        all_sizes[key] += val
    return all_sizes


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def write_output(out_df, implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    timestamp = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = 'allocations_{}.csv'.format(timestamp)
    path = os.path.join(implementation_dir, 'StatArbStrategy', 'allocations', file_name)
    out_df.to_csv(path, index=None)


def send_orders(out_df):
    client = ExecutionClient()
    for _, o in out_df.iterrows():
        shares = int(o.Dollars / o.RClose)
        if shares == 0:
            continue
        order = MOCOrder(symbol=o.Ticker,
                         quantity=shares,
                         strategy_id=o.Strategy)
        client.send_moc_order(order)
        print(order)

    client.send_transmit()
    client.close_zmq_sockets()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():

    import pdb; pdb.set_trace()

    raw_data = import_raw_data()

    run_map = import_run_map()

    models_params = import_models_params()

    positions = import_portfolio_manager_positions()

    strategy = StatArbImplementation()

    strategy.add_raw_data(raw_data)
    strategy.add_run_map(run_map)
    strategy.add_models_params(models_params)
    strategy.add_positions(positions)

    strategy.prep()

    #_ = raw_input("Press Enter to continue...")
    live_data = import_live_pricing()

    orders = strategy.run_live(live_data)
    out_df = live_data[['SecCode', 'Ticker', 'RClose']].merge(
        orders, how='outer')

    send_orders(out_df)

    # while True:
    #     try:
    #         live_data = import_live_pricing()

    #         orders = strategy.run_live(live_data)
    #         out_df = live_data[['SecCode', 'Ticker', 'RClose']].merge(
    #             orders, how='outer')

    #         send_orders(out_df)
    #         write_output(out_df)
    #         break

    #     except Exception as e:
    #         print(e)
    #         _ = raw_input("[ERROR] - Press any key to re-run and transmit")


if __name__ == '__main__':
    main()
