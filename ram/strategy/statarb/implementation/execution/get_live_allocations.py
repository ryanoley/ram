import os
import json
import pickle
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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_trading_dates():
    """
    Returns previous trading date, and current trading date
    """
    today = dt.date.today()
    dh = DataHandlerSQL()
    dates = dh.prior_trading_date([today, today+dt.timedelta(days=1)])
    return dates[0], dates[1]


def import_raw_data(implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    """
    Loads implementation raw data and processes it
    """
    statarb_path = os.path.join(implementation_dir,
                                'StatArbStrategy',
                                'daily_data')

    todays_files = get_todays_files(statarb_path)

    yesterday, today = get_trading_dates()

    output = {}
    print('Importing data...')
    for f in todays_files:
        name = format_data_name(f)
        data = import_format_raw_data(os.path.join(statarb_path, f))
        assert data.Date.max() == yesterday
        output[name] = data
    output['market_data'] = import_format_raw_data(
        os.path.join(statarb_path, 'market_index_data.csv'))
    return output


def get_todays_files(statarb_path):
    all_files = get_all_data_file_names(statarb_path)
    return get_max_date_files(all_files)


def get_all_data_file_names(data_dir_path):
    """
    Filters out market_index_data.csv
    """
    all_files = os.listdir(data_dir_path)
    all_files = [x for x in all_files if x.find('version') > 0]
    all_files.sort()
    return all_files


def get_max_date_files(all_files):
    max_date = max([x.split('_')[0] for x in all_files])
    todays_files = [x for x in all_files if x.find(max_date) > -1]
    todays_files = [x for x in todays_files if x.find('.csv') > -1]
    return todays_files


def format_data_name(file_name):
    return file_name[file_name.rfind('version'):].replace('.csv', '')


def import_format_raw_data(path):
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_pricing_data(implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    # SCALING DATA
    scaling_data = import_scaling_data(implementation_dir)
    bloomberg_data = import_bloomberg_data(implementation_dir)
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
    live_data = import_live_pricing(implementation_dir)
    data = live_data.merge(scaling)
    data['RClose'] = data.AdjClose
    data.AdjOpen = data.AdjOpen * data.PricingMultiplier
    data.AdjHigh = data.AdjHigh * data.PricingMultiplier
    data.AdjLow = data.AdjLow * data.PricingMultiplier
    data.AdjClose = data.AdjClose * data.PricingMultiplier
    data.AdjVwap = data.AdjVwap * data.PricingMultiplier
    data.AdjVolume = data.AdjVolume * data.VolumeMultiplier
    data = data.drop(['PricingMultiplier', 'VolumeMultiplier'], axis=1)

    # Write - This is used for analyzing our execution
    today = dt.datetime.utcnow()
    file_name = '{}_{}'.format(today.strftime('%Y%m%d'), 'live_pricing.csv')
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'daily_data',
                        file_name)
    live_data.to_csv(path, index=None)



    return data


def import_live_pricing(implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    dtypes = {'SecCode': str, 'Ticker': str, 'Issuer': str,
              'CLOSE': np.float64, 'LAST': np.float64, 'OPEN': np.float64,
              'HIGH': np.float64, 'LOW': np.float64, 'VWAP': np.float64,
              'VOLUME': np.float64}
    path = os.path.join(implementation_dir, 'StatArbStrategy',
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

    # Write to file - TODO: This should be moved elsewhere for performance
    # reasons
    today = dt.datetime.utcnow()
    file_name = '{}_{}'.format(today.strftime('%Y%m%d'),
                               'adj_live_pricing.csv')
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy',
                        'daily_data',
                        file_name)
    live_data.to_csv(path, index=None)
    return live_data


def import_scaling_data(implementation_dir):
    path = os.path.join(implementation_dir, 'StatArbStrategy',
                        'live_pricing', 'seccode_scaling.csv')
    data = pd.read_csv(path)
    data.SecCode = data.SecCode.astype(str)
    data.Date = convert_date_array(data.Date)
    data['QADirectDividendFactor'] = data.DividendFactor
    data = data[['SecCode', 'QADirectDividendFactor']]
    return data


def import_bloomberg_data(implementation_dir):
    dpath = os.path.join(implementation_dir, 'StatArbStrategy',
                         'live_pricing', 'bloomberg_scaling.csv')

    data = pd.read_csv(dpath)
    cols = np.array(['SecCode', 'DivMultiplier', 'SpinoffMultiplier',
                     'SplitMultiplier'])
    assert np.all(data.columns == cols)
    data.columns = ['SecCode', 'BbrgDivMultiplier', 'BbrgSpinoffMultiplier',
                    'BbrgSplitMultiplier']
    data.SecCode = data.SecCode.astype(str)
    return data[['SecCode', 'BbrgDivMultiplier', 'BbrgSpinoffMultiplier',
                 'BbrgSplitMultiplier']]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_run_map(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR,
        trained_model_dir_name=statarb_config.trained_models_dir_name):
    path = os.path.join(implementation_dir,
                        'StatArbStrategy',
                        'trained_models',
                        trained_model_dir_name,
                        'run_map.json')
    data = json.load(open(path, 'r'))
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_models_params(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR,
        trained_model_dir_name=statarb_config.trained_models_dir_name):
    """
    Returns
    -------
    output : dict
        Holds parameter and sklearn model for each trained model
    """
    path = os.path.join(implementation_dir,
                        'StatArbStrategy',
                        'trained_models',
                        trained_model_dir_name)
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
    all_files = [x for x in all_files if x.find('run_map.json') == -1]
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
    inds = [x.find('StatArb_') > -1 for x in positions.position]
    positions = positions.loc[inds].reset_index(drop=True)
    return positions


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class StatArbImplementation(object):

    def __init__(self, StatArbStrategy=StatArbStrategy):
        # Used for testing
        self.StatArbStrategy = StatArbStrategy

    def add_daily_data(self, daily_data):
        self.daily_data = daily_data

    def add_run_map_models(self, run_map, models):
        self.run_map = run_map
        self.models = models

    def add_positions(self, positions):
        self.positions = positions

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
            self.models_params_strategy[d['run_name']] = {}
            self.models_params_strategy[d['run_name']]['strategy'] = strategy
            self.models_params_strategy[d['run_name']]['column_params'] = \
                d['column_params']
            self.models_params_strategy[d['run_name']]['model'] = \
                self.models[d['run_name']]['model']
        print('Finished prepping data...')
        return

    def start(self):
        pass

    def run_live(self, live_data):

        orders = pd.DataFrame()

        ind = 0
        for name, objs in self.models_params_strategy.iteritems():
            strategy = objs['strategy']
            params = objs['column_params']
            model = objs['model']

            strategy.data.process_live_data(live_data)

            # Process params
            dparams = extract_params(params, strategy.data.get_args())
            strategy.data.set_args(live_flag=True, **dparams)

            sparams = extract_params(params, strategy.signals.get_args())
            strategy.signals.set_args(**sparams)

            strategy.signals.set_features(strategy.data.get_train_features())
            strategy.signals.set_test_data(strategy.data.get_test_data())
            strategy.signals.set_model(model)

            # strategy.constructor.set_test_dates(strategy.data.get_test_dates())

            # self.constructor.set_pricing_data(time_index,
            #                                   self.data.get_pricing_data())

            strategy.constructor.set_other_data(0, strategy.data.get_other_data())
            # Pass signals to portfolio constructor
            strategy.constructor.set_signal_data(0, strategy.signals.get_signals())

            cparams = extract_params(params, strategy.constructor.get_args())

            import pdb; pdb.set_trace()
            strategy.constructor.set_args(**cparams)

            sizes = strategy.constructor.get_day_position_sizes(
                date=0, column_index=ind)
            ind += 1

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

    raw_data = import_raw_data()

    run_map = import_run_map()

    models_params = import_models_params()

    positions = import_portfolio_manager_positions()

    # Add objects to Implementation instance
    strategy = StatArbImplementation()
    strategy.add_daily_data(raw_data)
    strategy.add_run_map_models(run_map, models_params)
    strategy.add_positions(positions)

    strategy.prep()

    #_ = raw_input("Press Enter to continue...")
    import pdb; pdb.set_trace()
    live_data = import_pricing_data()

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
