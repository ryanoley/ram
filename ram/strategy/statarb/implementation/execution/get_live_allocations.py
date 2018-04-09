import os
import sys
import json
import pickle
import logging
import numpy as np
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config
from ram.strategy.statarb.main import StatArbStrategy
from ram.data.data_handler_sql import DataHandlerSQL

from gearbox import convert_date_array

from ramex.orders.orders import MOCOrder
from ramex.client.client import ExecutionClient
from ramex.accounting.accounting import RamexAccounting
from ram.strategy.statarb.version_002.constructor.sizes import SizeContainer


###############################################################################
#  1. Import raw data
###############################################################################

def import_raw_data(data_dir=config.IMPLEMENTATION_DATA_DIR):
    files = get_todays_file_names(data_dir)
    # Get yesterday to confirm date
    yesterday = get_previous_trading_date()
    output = {}
    print('Importing data...')
    for f in files:
        name = clean_data_file_name(f)
        data = import_format_raw_data(f, data_dir)
        assert data.Date.max() == yesterday
        output[name] = data
    output['market_data'] = import_format_raw_data('market_index_data.csv')
    return output


def get_todays_file_names(data_dir=config.IMPLEMENTATION_DATA_DIR):
    data_dir = os.path.join(data_dir,
                            'StatArbStrategy',
                            'daily_data')
    timestamp = dt.date.today().strftime('%Y%m%d')
    files = [x for x in os.listdir(data_dir) if x.find(timestamp) > -1]
    files = [x for x in files if x.find('version') > -1]
    files.sort()
    assert len(files) > 0
    return files


def get_previous_trading_date():
    """
    Returns previous trading date, and current trading date
    """
    dh = DataHandlerSQL()
    return dh.prior_trading_date(dt.date.today())


def clean_data_file_name(file_name):
    return file_name[file_name.rfind('version'):].replace('.csv', '')


def import_format_raw_data(file_name,
                           data_dir=config.IMPLEMENTATION_DATA_DIR):
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'daily_data',
                        file_name)
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


###############################################################################
#  2. Import run map
###############################################################################

def import_run_map(data_path=config.IMPLEMENTATION_DATA_DIR,
                   models_dir=statarb_config.trained_models_dir_name):
    path = os.path.join(data_path,
                        'StatArbStrategy',
                        'trained_models',
                        models_dir,
                        'run_map.json')
    return json.load(open(path, 'r'))


###############################################################################
#  3. Import sklearn models and model parameters
###############################################################################

def import_models_params(data_path=config.IMPLEMENTATION_DATA_DIR,
                         models_dir=statarb_config.trained_models_dir_name):
    """
    Returns
    -------
    output : dict
        Holds parameter and sklearn model for each trained model
    """
    path = os.path.join(data_path,
                        'StatArbStrategy',
                        'trained_models',
                        models_dir)
    model_files, param_files = get_model_files(path)
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


def get_model_files(path):
    """
    Return file names from production trained models directories, and
    makes sure the model name is aligned with the param file name
    """
    all_files = os.listdir(path)
    all_files.remove('meta.json')
    all_files.remove('run_map.json')
    model_files = [x for x in all_files if x.find('skl_model') > -1]
    model_files.sort()
    param_files = [x for x in all_files if x.find('params') > -1]
    param_files.sort()
    # Assert that they are aligned
    for m, p in zip(model_files, param_files):
        assert m.replace('_skl_model.pkl', '') == p.replace('_params.json', '')
    return model_files, param_files


###############################################################################
#  4. StatArb positions
###############################################################################

def get_statarb_positions():
    positions = RamexAccounting('StatArb1').positions.copy()
    positions = positions[['SecCode', 'Ticker', 'Shares']]
    return positions


###############################################################################
#  5. Get SizeContainers
###############################################################################

def get_size_containers(data_path=config.IMPLEMENTATION_DATA_DIR,
                        models_dir=statarb_config.trained_models_dir_name):

    # Check to see if new SizeContainers need to be created
    models_path = os.path.join(data_path,
                               'StatArbStrategy',
                               'trained_models',
                               models_dir)

    # Check meta file to see if size containers need to be re-created
    meta_path = os.path.join(models_path, 'meta.json')
    meta = json.load(open(meta_path, 'r'))

    output = {}
    if meta['execution_confirm']:
        path = os.path.join(data_path,
                            'StatArbStrategy',
                            'size_containers')
        # TODO: Confirm this is correct file somewhere - pretrade_check?
        files = os.listdir(path)
        # Get files before today in case it is re-run
        today = dt.date.today().strftime('%Y%m%d')
        if max(files)[:8] == today:
            print('[WARNING] - SizeContainers for today already available.')
            print('            Using previous day\'s file.')
        file_name = max([x for x in files if x[:8] < today])
        containers_path = os.path.join(path, file_name)
        sizes = json.load(open(containers_path, 'r'))

        for k, v in sizes.iteritems():
            sc = SizeContainer(-1)
            sc.from_json(v)
            output[k] = sc

        return output

    # Otherwise
    _, param_files = get_model_files(models_path)
    for f in param_files:
        size_map = json.load(open(os.path.join(models_path, f), 'r'))
        sc = SizeContainer(-1)
        sc.from_json(size_map['sizes'])
        output[f.replace('_params.json', '')] = sc
    # Update meta file
    meta['execution_confirm'] = True
    json.dump(meta, open(meta_path, 'w'))
    write_new_size_containers(output, data_path)
    return output


def write_new_size_containers(size_containers,
                              data_dir=config.IMPLEMENTATION_DATA_DIR):
    # Write size_containers for yesterday's date (doesn't matter if it is
    # a weekend as above code select max timestamp)
    yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'size_containers',
                        '{}.json'.format(yesterday))
    output = {}
    for k, v in size_containers.iteritems():
        output[k] = v.to_json()
    json.dump(output, open(path, 'w'))


###############################################################################
#  6. StatArb Implementation Wrapper
###############################################################################

class StatArbImplementation(object):

    def __init__(self, StatArbStrategy=StatArbStrategy):
        self.StatArbStrategy = StatArbStrategy

    def add_daily_data(self, daily_data):
        self.daily_data = daily_data

    def add_run_map_models(self, run_map, models):
        self.run_map = run_map
        self.models = models

    def add_size_containers(self, size_containers):
        self.size_containers = size_containers

    def prep(self):
        assert hasattr(self, 'run_map')
        assert hasattr(self, 'daily_data')
        assert hasattr(self, 'models')
        print('Prepping data...')
        self.models_params_strategy = {}
        for d in self.run_map:
            strategy = self.StatArbStrategy(
                strategy_code_version=d['strategy_code_version']
            )
            strategy.strategy_init()
            strategy.data.prep_live_data(
                data=self.daily_data[d['prepped_data_version']],
                market_data=self.daily_data['market_data']
            )
            # Add size container to constructor
            strategy.constructor._size_containers[0] = \
                self.size_containers[d['run_name']]
            # Other
            self.models_params_strategy[d['run_name']] = {}
            self.models_params_strategy[d['run_name']]['strategy'] = strategy
            self.models_params_strategy[d['run_name']]['column_params'] = \
                d['column_params']
            self.models_params_strategy[d['run_name']]['model'] = \
                self.models[d['run_name']]['model']
        print('Finished prepping data...')
        return

    def run_live(self, live_data):

        orders = pd.DataFrame()

        models_params = self.models_params_strategy

        for name, objs in models_params.iteritems():

            # Extract parameters
            strategy = objs['strategy']
            params = objs['column_params']
            model = objs['model']

            # Derived class functionality from here on down
            strategy.data.process_live_data(live_data)

            # Process params
            dparams = extract_params(params, strategy.data.get_args())
            strategy.data.set_args(live_flag=True, **dparams)

            sparams = extract_params(params, strategy.signals.get_args())
            strategy.signals.set_args(**sparams)

            strategy.signals.set_features(strategy.data.get_train_features())
            strategy.signals.set_test_data(strategy.data.get_test_data())
            strategy.signals.set_model(model)

            strategy.constructor.set_test_dates(strategy.data.get_test_dates())
            strategy.constructor.set_other_data(strategy.data.get_other_data())
            strategy.constructor.set_signal_data(strategy.signals.get_signals())

            cparams = extract_params(params, strategy.constructor.get_args())
            strategy.constructor.set_args(**cparams)

            # Check size container is pulled
            sizes = strategy.constructor.get_day_position_sizes(
                dt.date.today(), 0)

            sizes = pd.Series(sizes).reset_index()
            sizes.columns = ['SecCode', 'Dollars']
            sizes['Strategy'] = name
            orders = orders.append(sizes)

        return orders


def extract_params(all_params, selected_params):
    out = {}
    for key in selected_params.keys():
        out[key] = all_params[key]
    return out


###############################################################################
#  6. Import sklearn models and model parameters
###############################################################################

def get_live_pricing_data():
    # SCALING DATA
    qad_scaling = import_qad_scaling_data()
    bloomberg_scaling = import_bloomberg_scaling_data()
    scaling = merge_scaling(qad_scaling, bloomberg_scaling)

    # IMPORT LIVE AND ADJUST
    live_data = import_live_pricing()
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


def import_qad_scaling_data():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'live_pricing',
                        'seccode_scaling.csv')
    data = pd.read_csv(path)
    data.SecCode = data.SecCode.astype(str)
    data = data.rename(columns={
        'DividendFactor': 'QADirectDividendFactor'
    })
    return data[['SecCode', 'QADirectDividendFactor']]


def import_bloomberg_scaling_data():
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy',
                         'live_pricing',
                         'bloomberg_scaling.csv')
    data = pd.read_csv(dpath)
    data.SecCode = data.SecCode.astype(str)
    data = data.rename(columns={
        'DivMultiplier': 'BbrgDivMultiplier',
        'SpinoffMultiplier': 'BbrgSpinoffMultiplier',
        'SplitMultiplier': 'BbrgSplitMultiplier'
    })
    return data


def merge_scaling(qad_scaling, bloomberg_scaling):
    data = qad_scaling.merge(bloomberg_scaling, how='left').fillna(1)
    data['PricingMultiplier'] = \
        data.QADirectDividendFactor * \
        data.BbrgDivMultiplier * \
        data.BbrgSpinoffMultiplier * \
        data.BbrgSplitMultiplier
    data['VolumeMultiplier'] = data.BbrgSplitMultiplier
    data = data[['SecCode', 'PricingMultiplier', 'VolumeMultiplier']]
    # Append index IDs with 1s as placeholders
    data_t = pd.DataFrame()
    data_t['SecCode'] = ['50311', '11113']
    data_t['PricingMultiplier'] = 1
    data_t['VolumeMultiplier'] = 1
    data = data.append(data_t).reset_index(drop=True)
    return data


def import_live_pricing(
        data_dir=os.path.join(os.getenv('DATA'), 'live_prices')):
    # Manually set column types
    dtypes = {'SecCode': str, 'Ticker': str, 'Issuer': str,
              'CLOSE': np.float64, 'LAST': np.float64, 'OPEN': np.float64,
              'HIGH': np.float64, 'LOW': np.float64, 'VWAP': np.float64,
              'VOLUME': np.float64}
    path = os.path.join(data_dir, 'prices.csv')
    data = pd.read_csv(path, na_values=['na'], dtype=dtypes)
    data.SecCode = data.SecCode.astype(str)
    data = data.rename(columns={
        'OPEN': 'AdjOpen',
        'HIGH': 'AdjHigh',
        'LOW': 'AdjLow',
        'LAST': 'AdjClose',
        'VOLUME': 'AdjVolume',
        'VWAP': 'AdjVwap',

    })
    data = data[['SecCode', 'Ticker', 'AdjOpen', 'AdjHigh',
                 'AdjLow', 'AdjClose', 'AdjVolume', 'AdjVwap']]
    # Write to file
    timestamp = dt.datetime.utcnow().strftime('%Y%m%d')
    file_name = '{}_live_pricing.csv'.format(timestamp)
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'daily_data',
                        file_name)
    data.to_csv(path, index=None)
    return data


###############################################################################
#  7. Cleanup - Sending and writing
###############################################################################

def send_orders(out_df, positions):

    orders = make_orders(out_df, positions)

    client = ExecutionClient()
    for o in orders:
        client.send_order(o)
        print(o)
    client.send_transmit('statArbBasket')
    client.close_zmq_sockets()


def make_orders(orders, positions):
    """
    LOGIC: Positions holds the shares we have. Orders holds the shares
    we want--from Positions.Quantity to Orders.Quantity.
    """
    # Drop anything with
    # Rollup and get shares
    orders['NewShares'] = (orders.Dollars / orders.RClose).astype(int)
    orders = orders.groupby('Ticker')['NewShares'].sum().reset_index()
    # Then net out/close shares
    data = orders.merge(positions, how='outer').fillna(0)
    data['TradeShares'] = data.NewShares - data.Shares
    output = []
    for _, o in data.iterrows():
        if o.TradeShares == 0:
            continue
        order = MOCOrder(basket='statArbBasket',
                         strategy_id='StatArb1',
                         symbol=o.Ticker,
                         quantity=o.TradeShares)
        output.append(order)
    return output


def write_output(out_df):
    timestamp = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = '{}_sent_allocations.csv'.format(timestamp)
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'allocations',
                        file_name)
    out_df.to_csv(path, index=None)


def write_size_containers(strategy,
                          data_dir=config.IMPLEMENTATION_DATA_DIR):
    today = dt.date.today().strftime('%Y%m%d')
    path = os.path.join(data_dir,
                        'StatArbStrategy',
                        'size_containers',
                        '{}_size_containers.json'.format(today))
    output = {}
    for k, v in strategy.size_containers.iteritems():
        output[k] = v.to_json()
    json.dump(output, open(path, 'w'))


###############################################################################
#  MAIN
###############################################################################

def main():

    ###########################################################################
    # 1. Import raw data
    raw_data = import_raw_data()

    # 2. Import raw data
    run_map = import_run_map()

    # 3. Import sklearn models and model parameters
    models_params = import_models_params()

    # 4. Get accounting information
    positions = get_statarb_positions()

    # 5. Get SizeContainers
    size_containers = get_size_containers()

    # 6. Prep data
    strategy = StatArbImplementation()
    strategy.add_daily_data(raw_data)
    strategy.add_run_map_models(run_map, models_params)
    strategy.add_size_containers(size_containers)
    strategy.prep()

    ###########################################################################

    _ = raw_input("Press Enter to continue...")

    while True:
        try:
            live_data = get_live_pricing_data()
            orders = strategy.run_live(live_data)
            out_df = orders.merge(live_data[['SecCode', 'Ticker', 'RClose']],
                                  how='left')
            send_orders(out_df, positions)
            write_output(out_df)
            write_size_containers(strategy)

            break

        except Exception as e:
            exc_info = sys.exc_info()
            print(e)
            print(logging.Formatter().formatException(exc_info))
            _ = raw_input("[ERROR] - Press any key to re-run and transmit")


if __name__ == '__main__':
    main()
