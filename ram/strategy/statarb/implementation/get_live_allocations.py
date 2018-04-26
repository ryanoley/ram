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

from ramex.orders.orders import MOCOrder, VWAPOrder
from ramex.application.client import ExecutionClient
from ramex.accounting.accounting import RamexAccounting
from ram.strategy.statarb.version_002.constructor.sizes import SizeContainer


LIVE_PRICES_DIR = os.path.join(os.getenv('DATA'), 'live_prices')

BASE_DIR = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy')

ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive')

LIVE_DIR = os.path.join(BASE_DIR, 'live')
# LIVE_DIR = os.path.join(ARCHIVE_DIR, 'live_directories', '20180425_live')


###############################################################################
#  0. Import raw data
###############################################################################

def get_position_size():
    return json.load(open(os.path.join(BASE_DIR, 'position_size.json')))


###############################################################################
#  1. Import raw data
###############################################################################

def import_raw_data():
    output = {}
    print('Importing data...')
    for f in get_todays_version_file_names():
        name = f.replace('.csv', '')
        data = import_format_raw_data(f)
        output[name] = data
    output['market_data'] = import_format_raw_data('market_index_data.csv')
    return output


def get_todays_version_file_names():
    all_files = os.listdir(LIVE_DIR)
    files = [x for x in all_files if x.find('version') > -1]
    files.sort()
    assert len(files) > 0
    return files


def import_format_raw_data(file_name):
    path = os.path.join(LIVE_DIR, file_name)
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


###############################################################################
#  2. Import run map
###############################################################################

def import_run_map():
    path = os.path.join(BASE_DIR,
                        'trained_models',
                        statarb_config.trained_models_dir_name,
                        'run_map.json')
    return json.load(open(path, 'r'))


###############################################################################
#  3. Import sklearn models and model parameters
###############################################################################

def import_models_params():
    """
    Returns
    -------
    output : dict
        Holds parameter and sklearn model for each trained model
    """
    model_files, param_files = get_model_files()
    output = {}
    path = os.path.join(BASE_DIR,
                        'trained_models',
                        statarb_config.trained_models_dir_name)
    print('Importing models and parameters...')
    for m, p in zip(model_files, param_files):
        run_name = m.replace('_skl_model.pkl', '')
        output[run_name] = {}
        output[run_name]['params'] = \
            json.load(open(os.path.join(path, p), 'r'))
        output[run_name]['model'] = \
            pickle.load(open(os.path.join(path, m), 'r'))
    return output


def get_model_files():
    """
    Return file names from production trained models directories, and
    makes sure the model name is aligned with the param file name
    """
    path = os.path.join(BASE_DIR,
                        'trained_models',
                        statarb_config.trained_models_dir_name)
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
    path = os.path.join(LIVE_DIR, 'eod_positions.csv')
    positions = pd.read_csv(path)
    positions = positions[positions.StrategyID == 'StatArb1']
    return positions


###############################################################################
#  5. Get SizeContainers
###############################################################################

def get_size_containers():
    path = os.path.join(LIVE_DIR, 'size_containers.json')
    sizes = json.load(open(path, 'r'))
    output = {}
    for k, v in sizes.iteritems():
        sc = SizeContainer(-1)
        sc.from_json(v)
        output[k] = sc
    return output


###############################################################################
#  6. Scaling data
###############################################################################

def import_scaling_data():
    # QAD SCALING
    path = os.path.join(LIVE_DIR, 'seccode_scaling.csv')
    data1 = pd.read_csv(path)
    data1.SecCode = data1.SecCode.astype(int).astype(str)
    data1 = data1.rename(columns={
        'DividendFactor': 'QADirectDividendFactor'
    })
    data1 = data1[['SecCode', 'QADirectDividendFactor']]
    # Bloomberg
    dpath = os.path.join(LIVE_DIR, 'bloomberg_scaling.csv')
    data2 = pd.read_csv(dpath)
    data2.SecCode = data2.SecCode.astype(int).astype(str)
    data2 = data2.rename(columns={
        'DivMultiplier': 'BbrgDivMultiplier',
        'SpinoffMultiplier': 'BbrgSpinoffMultiplier',
        'SplitMultiplier': 'BbrgSplitMultiplier'
    })
    # Merge
    data = data1.merge(data2, how='left').fillna(1)
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


###############################################################################
#  7. StatArb Implementation Wrapper
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

            strategy.constructor.set_test_dates(
                strategy.data.get_test_dates())
            strategy.constructor.set_other_data(
                strategy.data.get_other_data())
            strategy.constructor.set_signal_data(
                strategy.signals.get_signals())

            cparams = extract_params(params, strategy.constructor.get_args())
            strategy.constructor.set_args(**cparams)

            # Check size container is pulled
            sizes = strategy.constructor.get_day_position_sizes(
                dt.date.today(), 0)

            sizes = pd.Series(sizes).reset_index()
            sizes.columns = ['SecCode', 'PercAlloc']
            sizes['Strategy'] = name
            orders = orders.append(sizes)

        # Scale for number of models averaged together
        orders.PercAlloc /= float(len(models_params))

        # Clean out zero dollar allocations
        orders = orders[orders.PercAlloc != 0]

        return orders


def extract_params(all_params, selected_params):
    out = {}
    for key in selected_params.keys():
        out[key] = all_params[key]
    return out


###############################################################################
#  8. Live pricing
###############################################################################

def get_live_pricing_data(scaling):

    # FILL IN NAN VALUES AND PRINT TO SCREEN
    while True:
        # IMPORT LIVE AND ADJUST
        data = import_live_pricing()
        if np.any(data.isnull()):
            print('\n\nMISSING LIVE PRICES\n\n')
            input_ = raw_input("Press `y` to handle, otherwise will retry\n")
            if input_ == 'y':
                cols = ['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'AdjVwap']
                data.AdjOpen = data.AdjOpen.fillna(data[cols].mean(axis=1))
                data.AdjHigh = data.AdjHigh.fillna(data[cols].max(axis=1))
                data.AdjLow = data.AdjLow.fillna(data[cols].min(axis=1))
                data.AdjClose = data.AdjClose.fillna(data[cols].mean(axis=1))
                data.AdjVwap = data.AdjVwap.fillna(data[cols].mean(axis=1))
                break
        else:
            break

    # Archive
    if WRITE_FLAG:
        timestamp = dt.date.today().strftime('%Y%m%d')
        file_name = '{}_live_pricing.csv'.format(timestamp)
        path = os.path.join(ARCHIVE_DIR, 'live_pricing', file_name)
        data.to_csv(path, index=None)
        # Also in live directory
        dir_name = '{}_live'.format(timestamp)
        path = os.path.join(ARCHIVE_DIR, 'live_directories',
                            dir_name, 'prices.csv')
        data.to_csv(path, index=None)

    # Merge scaling
    data = data.merge(scaling)
    data['RClose'] = data.AdjClose
    data.AdjOpen = data.AdjOpen * data.PricingMultiplier
    data.AdjHigh = data.AdjHigh * data.PricingMultiplier
    data.AdjLow = data.AdjLow * data.PricingMultiplier
    data.AdjClose = data.AdjClose * data.PricingMultiplier
    data.AdjVwap = data.AdjVwap * data.PricingMultiplier
    data.AdjVolume = data.AdjVolume * data.VolumeMultiplier
    data = data.drop(['PricingMultiplier', 'VolumeMultiplier'], axis=1)

    return data


def import_live_pricing():
    # If running from archive, pull this live pricing
    if not WRITE_FLAG:
        path = os.path.join(LIVE_DIR, 'prices.csv')
        data = pd.read_csv(path)
        data.SecCode = data.SecCode.astype(str)
        return data

    # Manually set column types
    dtypes = {
        'SecCode': str,
        'Ticker': str,
        'Issuer': str,
        'CLOSE': np.float64,
        'LAST': np.float64,
        'OPEN': np.float64,
        'HIGH': np.float64,
        'LOW': np.float64,
        'VWAP': np.float64,
        'VOLUME': np.float64
    }
    path = os.path.join(LIVE_PRICES_DIR, 'prices.csv')
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

    return data


###############################################################################
#  9. Cleanup - Sending and writing
###############################################################################

def send_orders(out_df, positions):
    orders = make_orders(out_df, positions)
    # if not WRITE_FLAG:
    #     return
    client = ExecutionClient()
    for o in orders:
        client.send_order(o)
    client.send_transmit('statArbBasket')
    client.close_zmq_sockets()
    print('Order transmission complete')


def make_orders(orders, positions):
    """
    LOGIC: Positions holds the shares we have. Orders holds the shares
    we want--from Positions.Quantity to Orders.Quantity.
    """
    # Drop anything with
    dollar_order = orders.Dollars.abs().sum()
    # Rollup and get shares
    orders['NewShares'] = (orders.Dollars / orders.RClose).astype(int)
    orders = orders.groupby('Ticker')['NewShares'].sum().reset_index()
    # Print some stats
    print('\nOrder Stats')
    print('Order in Dollars: {}'.format(dollar_order))
    print('Long Orders: {}'.format((orders.NewShares > 0).sum()))
    print('Long Shares: {}'.format(orders[orders.NewShares > 0].NewShares.sum()))
    print('Short Orders: {}'.format((orders.NewShares < 0).sum()))
    print('Short Shares: {}'.format(orders[orders.NewShares < 0].NewShares.sum()))
    print('\n')

    # Then net out/close shares
    data = orders.merge(positions, how='outer').fillna(0)
    data['TradeShares'] = data.NewShares - data.Shares
    output = []
    # Start/End time
    now = dt.datetime.now()
    start_time = now + dt.timedelta(minutes=1)
    start_time = dt.time(start_time.hour, start_time.minute)
    end_time = now + dt.timedelta(minutes=4)
    end_time = dt.time(end_time.hour, end_time.minute)

    for _, o in data.iterrows():
        if o.TradeShares == 0:
            continue

        # order = MOCOrder(basket='statArbBasket',
        #                  strategy_id='StatArb1',
        #                  symbol=o.Ticker,
        #                  quantity=o.TradeShares)
        order = VWAPOrder(basket='statArbBasket',
                          strategy_id='StatArb1',
                          symbol=o.Ticker,
                          quantity=o.TradeShares,
                          start_time=start_time,
                          end_time=end_time,
                          participation=20)
        output.append(order)

    return output


def write_output(out_df):
    if not WRITE_FLAG:
        return
    timestamp = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = '{}_sent_allocations.csv'.format(timestamp)
    path = os.path.join(ARCHIVE_DIR, 'allocations', file_name)
    out_df.to_csv(path, index=None)


def write_size_containers(strategy):
    if WRITE_FLAG:
        return
    today = dt.date.today().strftime('%Y%m%d')
    path = os.path.join(ARCHIVE_DIR, 'size_containers',
                        '{}_size_containers.json'.format(today))
    output = {}
    for k, v in strategy.size_containers.iteritems():
        output[k] = v.to_json()
    json.dump(output, open(path, 'w'))


###############################################################################
#  MAIN
###############################################################################

def main():

    # Infer if reading from archive or live directory
    global WRITE_FLAG
    WRITE_FLAG = True if LIVE_DIR.find('archive') == -1 else False

    ###########################################################################
    # 0. Checks meta and import position size
    position_size = get_position_size()

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

    # 6. Scaling data for live data
    scaling = import_scaling_data()

    # 7. Prep data
    strategy = StatArbImplementation()
    strategy.add_daily_data(raw_data)
    strategy.add_run_map_models(run_map, models_params)
    strategy.add_size_containers(size_containers)
    strategy.prep()

    ###########################################################################

    _ = raw_input("Press Enter to continue...")

    while True:
        try:
            # 8. Live pricing
            live_data = get_live_pricing_data(scaling)
            orders = strategy.run_live(live_data)

            out_df = orders.merge(live_data[['SecCode', 'Ticker', 'RClose']],
                                  how='left')
            out_df['Dollars'] = \
                out_df.PercAlloc * position_size['gross_position_size']

            send_orders(out_df, positions)

            # 9. Writing and cleanup
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
