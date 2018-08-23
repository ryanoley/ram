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

from ramex.orders.orders import LOCOrder, VWAPOrder
from ramex.application.client import ExecutionClient

from ram.strategy.statarb.objects.sizes import SizeContainer


LIVE_PRICES_DIR = os.path.join(os.getenv('DATA'), 'live_prices')

IMP_DIR = os.path.join(config.IMPLEMENTATION_DATA_DIR)

BASE_DIR = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy')

STRATEGY_ID = 'StatArb0001'


###############################################################################
#  0. Import raw data
###############################################################################

def get_position_size(data_dir=BASE_DIR):
    return json.load(open(os.path.join(data_dir, 'position_size.json')))


###############################################################################
#  1. Import raw data
###############################################################################

def import_raw_data(data_dir=BASE_DIR):
    output = {}
    print('Importing data...')
    for f in get_todays_version_file_names(data_dir):
        name = f.replace('.csv', '')
        data = import_format_raw_data(f, data_dir)
        output[name] = data
    output['market_data'] = import_format_raw_data('market_index_data.csv',
                                                   data_dir)
    return output


def get_todays_version_file_names(data_dir=BASE_DIR):
    all_files = os.listdir(os.path.join(data_dir, 'live'))
    files = [x for x in all_files if x.find('version') > -1]
    files.sort()
    assert len(files) > 0
    return files


def import_format_raw_data(file_name, data_dir=BASE_DIR):
    path = os.path.join(data_dir, 'live', file_name)
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_raw_data_archive(recon_date, data_dir=BASE_DIR):
    output = {}
    print('Importing data...')
    dir_name = '{}_live'.format(recon_date.strftime('%Y%m%d'))
    dir_path = os.path.join(data_dir, 'archive',
                            'live_directories', dir_name)

    for f in get_archive_version_file_names(dir_path):
        name = f.replace('.csv', '')
        data = import_format_raw_data_archive(f, dir_path)
        output[name] = data
    output['market_data'] = import_format_raw_data_archive(
        'market_index_data.csv', dir_path)
    return output


def get_archive_version_file_names(dir_path):
    all_files = os.listdir(dir_path)
    files = [x for x in all_files if x.find('version') > -1]
    files.sort()
    assert len(files) > 0
    return files


def import_format_raw_data_archive(file_name, dir_path):
    path = os.path.join(dir_path, file_name)
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


###############################################################################
#  2. Import run map
###############################################################################

def import_run_map(model_dir_name=statarb_config.trained_models_dir_name,
                   data_dir=BASE_DIR):
    path = os.path.join(data_dir,
                        'trained_models',
                        model_dir_name,
                        'run_map.json')
    return json.load(open(path, 'r'))


###############################################################################
#  3. Import sklearn models and model parameters
###############################################################################

def import_models_params(
        models_dir_name=statarb_config.trained_models_dir_name,
        data_dir=BASE_DIR):
    """
    Returns
    -------
    output : dict
        Holds parameter and sklearn model for each trained model
    """
    path, model_files, param_files = get_model_files(models_dir_name, data_dir)
    print('Importing models and parameters...')
    output = {}
    for m, p in zip(model_files, param_files):
        run_name = m.replace('_skl_model.pkl', '')
        output[run_name] = {}
        output[run_name]['params'] = \
            json.load(open(os.path.join(path, p), 'r'))
        output[run_name]['model'] = \
            pickle.load(open(os.path.join(path, m), 'r'))
    return output


def get_model_files(models_dir_name, data_dir):
    """
    Return file names from production trained models directories, and
    makes sure the model name is aligned with the param file name
    """
    path = os.path.join(data_dir, 'trained_models', models_dir_name)
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
    return path, model_files, param_files


###############################################################################
#  4. StatArb positions
###############################################################################

def get_statarb_positions(data_dir=IMP_DIR):
    path = os.path.join(data_dir, 'StatArbStrategy',
                        'live', 'eod_positions.csv')
    positions = pd.read_csv(path)
    positions = positions[positions.StrategyID == STRATEGY_ID]
    positions.SecCode = positions.SecCode.astype(int).astype(str)
    positions = positions[['SecCode', 'Ticker', 'Shares']].copy()
    # Substitute in EzeRealTick tickers
    path = os.path.join(data_dir, 'qad_to_eze_ticker_map.json')
    mapping = json.load(open(path, 'r'))
    positions.Ticker = positions.Ticker.replace(mapping)
    return positions


###############################################################################
#  5. Get SizeContainers
###############################################################################

def get_size_containers(data_dir=BASE_DIR):
    path = os.path.join(data_dir, 'live', 'size_containers.json')
    sizes = json.load(open(path, 'r'))
    output = {}
    for k, v in sizes.iteritems():
        sc = SizeContainer(-1)
        sc.from_json(v)
        output[k] = sc
    return output


def get_size_containers_archive(recon_date, data_dir=BASE_DIR):
    dir_name = '{}_live'.format(recon_date.strftime('%Y%m%d'))
    path = os.path.join(data_dir, 'archive', 'live_directories',
                        dir_name, 'size_containers.json')
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

def import_scaling_data(data_dir=BASE_DIR):
    # QAD SCALING
    path1 = os.path.join(data_dir, 'live', 'seccode_scaling.csv')
    # Bloomberg
    path2 = os.path.join(data_dir, 'live', 'bloomberg_scaling.csv')
    return _format_scaling_data(path1, path2)


def import_scaling_data_archive(recon_date, data_dir=BASE_DIR):
    dir_name = '{}_live'.format(recon_date.strftime('%Y%m%d'))
    path = os.path.join(data_dir, 'archive', 'live_directories', dir_name)
    path1 = os.path.join(path, 'seccode_scaling.csv')
    path2 = os.path.join(path, 'bloomberg_scaling.csv')
    return _format_scaling_data(path1, path2)


def _format_scaling_data(path1, path2):
    data1 = pd.read_csv(path1)
    data1.SecCode = data1.SecCode.astype(int).astype(str)
    data1 = data1.rename(columns={
        'DividendFactor': 'QADirectDividendFactor'
    })
    data1 = data1[['SecCode', 'QADirectDividendFactor']]
    data2 = pd.read_csv(path2)
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

    def add_drop_short_seccodes(self, drop_short_seccodes):
        self.drop_short_seccodes = drop_short_seccodes

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
                dt.date.today(), 0, self.drop_short_seccodes)

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

def archive_live_pricing(data, data_dir):
    timestamp = dt.date.today().strftime('%Y%m%d')
    file_name = '{}_live_pricing.csv'.format(timestamp)
    path = os.path.join(data_dir, 'archive', 'live_pricing', file_name)
    data.to_csv(path, index=None)
    # Also in live directory
    dir_name = '{}_live'.format(timestamp)
    path = os.path.join(data_dir, 'archive', 'live_directories',
                        dir_name, 'prices.csv')
    data.to_csv(path, index=None)
    return


def import_live_pricing(live_prices_dir=LIVE_PRICES_DIR):
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

    path = os.path.join(live_prices_dir, 'prices.csv')
    data = pd.read_csv(path, na_values=['na'], dtype=dtypes)
    data.SecCode = data.SecCode.astype(str)
    data = data.rename(columns={
        'OPEN': 'ROpen',
        'HIGH': 'RHigh',
        'LOW': 'RLow',
        'LAST': 'RClose',
        'VOLUME': 'RVolume',
        'VWAP': 'RVwap',

    })
    data = data[['SecCode', 'Ticker', 'ROpen', 'RHigh', 'RLow', 'RClose',
                 'RVolume', 'RVwap']]
    # Add file creation datetime
    time_modified = dt.datetime.fromtimestamp(os.path.getmtime(path))
    data['captured_time'] = time_modified
    return data


def get_live_pricing_data(scaling, data_dir=BASE_DIR):

    # FILL IN NAN VALUES AND PRINT TO SCREEN
    while True:
        # IMPORT LIVE AND ADJUST
        data = import_live_pricing()
        if np.any(data.isnull()):
            print('\n\nMISSING LIVE PRICES\n\n')
            input_ = raw_input("Press `y` to handle, otherwise will retry\n")
            if input_ == 'y':
                cols = ['ROpen', 'RHigh', 'RLow', 'RClose', 'RVwap']
                data.ROpen = data.ROpen.fillna(data[cols].mean(axis=1))
                data.RHigh = data.RHigh.fillna(data[cols].max(axis=1))
                data.RLow = data.RLow.fillna(data[cols].min(axis=1))
                data.RClose = data.RClose.fillna(data[cols].mean(axis=1))
                data.RVwap = data.RVwap.fillna(data[cols].mean(axis=1))
                break
        else:
            break

    archive_live_pricing(data, data_dir)

    data.drop('captured_time', axis=1, inplace=True)
    # Merge scaling
    data = data.merge(scaling)
    data['AdjOpen'] = data.ROpen * data.PricingMultiplier
    data['AdjHigh'] = data.RHigh * data.PricingMultiplier
    data['AdjLow'] = data.RLow * data.PricingMultiplier
    data['AdjClose'] = data.RClose * data.PricingMultiplier
    data['AdjVwap'] = data.RVwap * data.PricingMultiplier
    data['AdjVolume'] = data.RVolume * data.VolumeMultiplier
    data = data.drop(['PricingMultiplier', 'VolumeMultiplier', 'ROpen',
                     'RHigh', 'RLow', 'RVwap'], axis=1)

    return data


###############################################################################
#  9. Cleanup - Sending and writing
###############################################################################

def send_orders(orders):
    client = ExecutionClient(STRATEGY_ID, production_flag=True)
    for o in orders:
        client.send_order(o)
    client.send_transmit('statArbBasket')
    client.close_zmq_sockets()
    print('Order transmission complete')


def make_orders(orders, positions, pricing, drop_short_seccodes,
                loc_price_pct=.05, volume_pct_lim=.05):
    """
    LOGIC: Positions holds the shares we have. Orders holds the shares
    we want--from Positions.Quantity to Orders.Quantity.
    """
    # Rollup and get shares
    orders['NewShares'] = (orders.Dollars / orders.RClose).astype(int)
    grp_orders = orders.groupby(['Ticker', 'SecCode'])
    grp_orders = grp_orders['NewShares', 'PercAlloc', 'Dollars'].sum()
    grp_orders = grp_orders.reset_index()

    # Drop shorts
    grp_orders = check_dropped_seccodes(grp_orders, drop_short_seccodes)

    # Then net out/close shares
    data = grp_orders.merge(positions, how='outer').fillna(0)
    data = data.merge(pricing[['SecCode', 'RClose', 'RVolume']],
                      how='left').fillna(0)
    data['TradeShares'] = data.NewShares - data.Shares

    print('#########################')
    print(' POSITION STATS')
    print(' Open Longs: {}'.format((data.NewShares > 0).sum()))
    print(' Open Shorts: {}'.format((data.NewShares < 0).sum()))
    print('\n')

    # Print some stats
    print('#########################')
    print(' ORDER STATS')
    print(' Long Orders: {}'.format((data.TradeShares > 0).sum()))
    print(' Long Shares: {}'.format(
                                data[data.TradeShares > 0].TradeShares.sum()))
    print(' Short Orders: {}'.format((data.TradeShares < 0).sum()))
    print(' Short Shares: {}'.format(
                                data[data.TradeShares < 0].TradeShares.sum()))
    print('\n')
    print('#########################')
    print(' VWAP ORDER SUBSTITUTIONS')

    output = []
    for _, o in data.iterrows():
        order_qty = o.TradeShares
        last_price = o.RClose
        last_volume = 1 if o.RVolume == 0 else o.RVolume
        perc_of_vol = abs(order_qty) / last_volume

        # Set limit price for LOC Orders, must be in 5 cent increments
        if order_qty == 0:
            continue
        elif order_qty > 0:
            limit = round(last_price * (1 + loc_price_pct) / .05) * .05
        else:
            limit = round(last_price * (1 - loc_price_pct) / .05) * .05

        if (last_price == 0) | (perc_of_vol > volume_pct_lim):
            order = VWAPOrder(basket='statArbBasket',
                              strategy_id=STRATEGY_ID,
                              symbol=o.Ticker,
                              quantity=o.TradeShares,
                              start_time=dt.time(15, 45),
                              end_time=dt.time(16, 00),
                              participation=20)
            print 'Tkr: {} Vol%: {} Close: {}'.format(o.Ticker,
                                                      perc_of_vol,
                                                      last_price)
        else:
            order = LOCOrder(basket='statArbBasket',
                             strategy_id=STRATEGY_ID,
                             symbol=o.Ticker,
                             quantity=o.TradeShares,
                             limit_price=np.round(limit, 2))

        output.append(order)

    return output, data


def write_output(out_df, data_dir=BASE_DIR):
    timestamp = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = '{}_sent_allocations.csv'.format(timestamp)
    path = os.path.join(data_dir, 'archive', 'allocations', file_name)
    out_df.to_csv(path, index=None)


def write_size_containers(strategy, data_dir=BASE_DIR):
    today = dt.date.today().strftime('%Y%m%d')
    path = os.path.join(data_dir, 'archive', 'size_containers',
                        '{}_size_containers.json'.format(today))
    output = {}
    for k, v in strategy.size_containers.iteritems():
        output[k] = v.to_json()
    json.dump(output, open(path, 'w'))


def get_short_sell_kill_seccodes(data_dir=IMP_DIR):
    # Read in short sell seccode csv and return list of seccodes
    path = os.path.join(data_dir, 'short_sell_kill_list.csv')
    data = pd.read_csv(path)
    data = data[data.SecCode.notnull()]
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data.SecCode.values.tolist()


def check_dropped_seccodes(orders, drop_short_seccodes):
    # Drop those SecCodes in the list and those that are short
    drop_seccodes = orders.SecCode.isin(drop_short_seccodes) & \
        (orders.NewShares < 0)
    if np.any(drop_seccodes):
        drops = orders[drop_seccodes]
        print('\n')
        print('#########################')
        print(' Dropping Impossible-to-Borrow and Expensive SecCodes: ')
        print(drops.SecCode.unique().tolist())
        print(' Net Dollars Dropped (Positive Number requires Long Hedge): {}'.format(round(drops.Dollars.sum())))
        print(' Total Gross Dollars Dropped: {}'.format(round(drops.Dollars.abs().sum())))
        print('\n')

    return orders[~drop_seccodes].reset_index(drop=True)

###############################################################################
#  MAIN
###############################################################################


def confirm_prep_data():
    meta = json.load(open(os.path.join(BASE_DIR, 'live', 'meta.json'), 'r'))
    t = meta['prepped_date']
    # Confirm prep data was run today
    t = dt.date(int(t[:4]), int(t[4:6]), int(t[6:]))
    assert t == dt.date.today(), 'Run prep_data.py!!'
    assert statarb_config.trained_models_dir_name == \
        meta['trained_models_dir_name']


def main():

    confirm_prep_data()

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

    # 7. Drop SecCodes
    drop_short_seccodes = get_short_sell_kill_seccodes()

    # 8. Prep data
    strategy = StatArbImplementation()
    strategy.add_daily_data(raw_data)
    strategy.add_run_map_models(run_map, models_params)
    strategy.add_size_containers(size_containers)
    strategy.add_drop_short_seccodes(drop_short_seccodes)
    strategy.prep()

    import pdb; pdb.set_trace()

    ###########################################################################
    _ = raw_input("Press Enter to continue...EXPORT LIVE PRICES FIRST!")

    while True:
        try:
            # 8. Live pricing
            live_data = get_live_pricing_data(scaling)
            orders = strategy.run_live(live_data)

            out_df = orders.merge(live_data[['SecCode', 'Ticker', 'RClose']],
                                  how='left')
            out_df['Dollars'] = \
                out_df.PercAlloc * position_size['gross_position_size']

            order_list, order_df = make_orders(out_df, positions, live_data,
                                               drop_short_seccodes)
            send_orders(order_list)

            write_output(order_df)
            write_size_containers(strategy)

            break

        except Exception as e:
            exc_info = sys.exc_info()
            print(e)
            print(logging.Formatter().formatException(exc_info))
            _ = raw_input("[ERROR] - Press any key to re-run and transmit")


if __name__ == '__main__':
    main()
