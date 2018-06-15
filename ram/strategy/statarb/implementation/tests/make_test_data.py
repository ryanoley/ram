import os
import json
import shutil
import pickle
import numpy as np
import pandas as pd
import datetime as dt

from sklearn.linear_model import LinearRegression

from ram.data.data_handler_sql import DataHandlerSQL

from ram.strategy.statarb.objects.sizes import SizeContainer
from ram.strategy.statarb.implementation.prep_data import get_trading_dates


class ImplementationDataTestSuite(object):

    def __init__(self):
        self.data_dir = os.path.join(
            os.getenv('GITHUB'), 'ram', 'ram', 'test_implementation_data')
        dates = get_trading_dates()
        self.yesterday = dates[0]
        self.today = dates[1]

    def make_data(self):
        self._init_dirs()
        self._make_killed_seccodes()
        self._make_trained_models_data()
        self._make_version_data_files()
        self._make_live_data_files()
        self._make_scaling_data_files()
        self._make_ticker_mapping()
        self._make_position_sheet_files()
        self._make_size_containers()
        self._make_short_locate_data()
        self._make_short_sell_kill_list()

    def delete_data(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

    def _init_dirs(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

        os.mkdir(self.data_dir)

        path = os.path.join(self.data_dir, 'StatArbStrategy')
        os.mkdir(path)

        # Trained Models
        path1 = os.path.join(path, 'trained_models')
        os.mkdir(path1)
        path1 = os.path.join(path, 'trained_models', 'models_0005')
        os.mkdir(path1)

        # Live Data
        path1 = os.path.join(path, 'live')
        os.mkdir(path1)

        # Archive Data
        path1 = os.path.join(path, 'archive')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'allocations')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'bloomberg_scaling')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'live_pricing')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'qad_scaling')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'size_containers')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'qad_seccode_data')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'version_data')
        os.mkdir(path1)
        path1 = os.path.join(path, 'archive', 'locates')
        os.mkdir(path1)

        # Live pricing - OUTSIDE RAM
        path = os.path.join(self.data_dir, 'live_prices')
        os.mkdir(path)

        # Ramex pricing - OUTSIDE RAM
        path = os.path.join(self.data_dir, 'ramex')
        os.mkdir(path)
        path1 = os.path.join(path, 'eod_positions')
        os.mkdir(path1)

    def _make_killed_seccodes(self):
        killed = {'123': '2010-01-01'}
        path = os.path.join(self.data_dir,
                            'StatArbStrategy',
                            'killed_seccodes.json')
        with open(path, 'w') as outfile:
            outfile.write(json.dumps(killed))

    def _make_trained_models_data(self):
        path = os.path.join(self.data_dir, 'StatArbStrategy')
        path1 = os.path.join(path, 'trained_models', 'models_0005')

        # Create sklearn model and params
        model = LinearRegression()
        X = np.random.randn(100, 3)
        y = np.random.randn(100)
        model.fit(X=X, y=y)

        pathm = os.path.join(path1,
                             'StatArbStrategy_run_0003_1000_skl_model.pkl')
        with open(pathm, 'w') as outfile:
            outfile.write(pickle.dumps(model))

        pathm = os.path.join(path1, 'StatArbStrategy_run_009_12_skl_model.pkl')
        with open(pathm, 'w') as outfile:
            outfile.write(pickle.dumps(model))

        # Get last two days
        t1 = (self.yesterday - dt.timedelta(days=1)).strftime('%Y%m%d')
        t2 = self.yesterday.strftime('%Y%m%d')
        params = {
            'params': {'v1': 10, 'v2': 30},
            'sizes': {
                'dates': [t1, t2],
                'n_days': 10,
                'sizes': {
                    t1: {'A': 100000, 'B': -200000},
                    t2: {'A': 200000, 'B': -300000}
                }
            }
        }
        pathm = os.path.join(path1,
                             'StatArbStrategy_run_0003_1000_params.json')
        with open(pathm, 'w') as outfile:
            outfile.write(json.dumps(params))

        params = {
            'params': {'v1': 10, 'v2': 30},
            'sizes': {
                'dates': [t1, t2],
                'n_days': 5,
                'sizes': {
                    t1: {'A': -400000, 'B': 500000},
                    t2: {'A': -500000, 'B': 800000}
                }
            }
        }
        pathm = os.path.join(path1, 'StatArbStrategy_run_009_12_params.json')
        with open(pathm, 'w') as outfile:
            outfile.write(json.dumps(params))

        # Run map
        params = {'blueprint': {
            'universe_filter_arguments':
                {'filter': 'AvgDolVol',
                 'where': 'MarketCap >= 200 and Close_ between 5 and 500',
                 'univ_size': 800},
            'features': ['AdjOpen', 'AdjHigh'],
            'constructor_type': 'universe',
            'output_dir_name': 'StatArbStrategy',
            'universe_date_parameters': {
                'quarter_frequency_month_offset': 0,
                'start_year': 2004, 'frequency': 'M',
                'train_period_length': 3,
                'test_period_length': 2},
            'market_data_params': {
                'features': ['AdjClose'],
                'seccodes': [50311, 11113, 11097, 11099, 111000]},
                'description': 'Sector 20, Version 002'
            },
            'prepped_data_version': 'version_0010',
            'column_params': {
                'response_days': 5, 'holding_period': 9,
                'response_type': 'Simple', 'per_side_count': 30,
                'model': {'max_features': 0.8, 'type': 'tree',
                          'min_samples_leaf': 500}, 'score_var': 'prma_15'},
            'stack_index': 'version_002~version_0010',
            'run_name': 'StatArbStrategy_run_0003_1000',
            'strategy_code_version': 'version_002'
        }
        params2 = params.copy()
        params2['prepped_data_version'] = 'version_0018'
        params2['run_name'] = 'StatArbStrategy_run_009_12'
        params2['stack_index'] = 'version_002~version_0018',
        run_map = [params, params2]

        # Write
        with open(os.path.join(path1, 'run_map.json'), 'w') as outfile:
            outfile.write(json.dumps(run_map))

        meta = {'execution_confirm': False}
        with open(os.path.join(path1, 'meta.json'), 'w') as outfile:
            outfile.write(json.dumps(meta))

    def _make_version_data_files(self):
        path = os.path.join(self.data_dir, 'StatArbStrategy')
        path1 = os.path.join(path, 'live')
        # Raw Data
        data = pd.DataFrame()
        yesterday = self.yesterday
        data['Date'] = [yesterday - dt.timedelta(days=2),
                        yesterday - dt.timedelta(days=1),
                        yesterday] * 2
        data['SecCode'] = [14141.0] * 3 + ['43242'] * 3
        data['AdjClose'] = range(6)
        today = dt.date.today().strftime('%Y%m%d')
        data.to_csv(os.path.join(path1, 'version_0010.csv'), index=False)
        data.to_csv(os.path.join(path1, 'version_0018.csv'), index=False)
        data['SecCode'] = ['1'] * 3 + ['2'] * 3
        data.to_csv(os.path.join(path1, 'market_index_data.csv'), index=False)
        # Make live directory meta file
        meta = {'trained_models_dir_name': 'models_0005',
                'prepped_date': dt.date.today().strftime('%Y%m%d')}
        with open(os.path.join(path1, 'meta.json'), 'w') as outfile:
            outfile.write(json.dumps(meta))

    def _make_live_data_files(self):
        path = os.path.join(self.data_dir, 'live_prices')
        # Live pricing
        data = pd.DataFrame()
        data['SecCode'] = [1234, 4242, 3535]
        data['Ticker'] = ['TRUE', 'IBM', 'GOOGL']
        data['Issuer'] = ['TRUESOMETHING', 'IBM Corp', 'Alphabet']
        data['CLOSE'] = [1, 2, 3]
        data['LAST'] = [1, 2, 'na']
        data['OPEN'] = [1, 2, 3]
        data['HIGH'] = [1, 2, 3]
        data['LOW'] = [1, 2, 'na']
        data['VWAP'] = [1, np.nan, 3]
        data['VOLUME'] = [None, 2, 3]
        data.to_csv(os.path.join(path, 'prices.csv'), index=None)

    def _make_scaling_data_files(self):
        path = os.path.join(self.data_dir, 'StatArbStrategy')
        path1 = os.path.join(path, 'live')
        # Scaling data
        data = pd.DataFrame()
        data['SecCode'] = [1234, 4242, 3535]
        data['Date'] = '2010-01-01'
        data['DividendFactor'] = [1, 1.1, 1.2]
        data['SplitFactor'] = [1, 1.1, 1.2]
        data.to_csv(os.path.join(path1, 'seccode_scaling.csv'), index=None)
        # Bloomberg data
        data = pd.DataFrame()
        data['SecCode'] = [5151, 72727]
        data['DivMultiplier'] = [1., 1.1]
        data['SpinoffMultiplier'] = [1., 1.]
        data['SplitMultiplier'] = [1., 2.]
        data.to_csv(os.path.join(path1, 'bloomberg_scaling.csv'), index=None)

    def _make_ticker_mapping(self):
        path = os.path.join(self.data_dir, 'StatArbStrategy')
        path1 = os.path.join(path, 'live')
        data = pd.DataFrame()
        data['SecCode'] = [101, 201, 301, 401]
        data['Ticker'] = ['A', 'B', 'C', 'D']
        data['Cusip'] = ['A0000001', 'B0000002', 'C0000003', 'D0000004']
        data['Issuer'] = ['IsrA', 'IsrB', 'IsrC', 'IsrD']
        data.to_csv(os.path.join(path1, 'qad_seccode_data.csv'),
                    index=None)

    def _make_position_sheet_files(self):
        data = pd.DataFrame()
        data['position'] = ['5050_StatArb_A0123', '1010_StatArb_A0123',
                            'GE Special Sit', 'CUDA Earnings']
        data['symbol'] = ['AAPL', 'IBM', 'GE', 'CUDA']
        data['share_count'] = [1000, -1000, 100, 3333]
        data['market_price'] = [10, 30, 10, 20]
        data['position_value'] = [10000, -30000, 330303, -1292]
        data['daily_pl'] = [20303, -2032, 3, 1]
        data['position_value_perc_aum'] = [0.003, 0.001, 0.1, 10.]
        # StatArb Live
        path = os.path.join(self.data_dir, 'StatArbStrategy', 'live')
        data.to_csv(os.path.join(path, 'eod_positions.csv'), index=0)
        # In Archive
        path = os.path.join(self.data_dir, 'ramex', 'eod_positions')
        timestamp = self.yesterday.strftime('%Y%m%d')
        file_name = '{}_positions.csv'.format(timestamp)
        data.to_csv(os.path.join(path, file_name), index=None)

    def _make_size_containers(self):
        dpath = os.path.join(self.data_dir,
                             'StatArbStrategy',
                             'archive',
                             'size_containers')
        prefix = self.yesterday.strftime('%Y%m%d')
        file_name = '{}_size_containers.json'.format(prefix)
        t1 = (self.yesterday - dt.timedelta(days=1)).strftime('%Y%m%d')
        t2 = self.yesterday.strftime('%Y%m%d')
        sizes = {'model1': {
            'dates': [t1, t2],
            'n_days': 10,
            'sizes': {
                t1: {'A': 100000, 'B': -200000},
                t2: {'A': 200000, 'B': -300000}
            }
        }}
        json.dump(sizes, open(os.path.join(dpath, file_name), 'w'))
        # In live directory
        path = os.path.join(self.data_dir,
                            'StatArbStrategy',
                            'live',
                            'size_containers.json')
        sizes = SizeContainer(2)
        t = self.yesterday - dt.timedelta(days=1)
        sizes.update_sizes({'A': 100, 'B': 200}, t)
        t = self.yesterday
        sizes.update_sizes({'A': 100, 'B': 200}, t)
        sizes = {'StatArbStrategy_run_0003_1000': sizes.to_json(),
                 'StatArbStrategy_run_009_12': sizes.to_json()}
        json.dump(sizes, open(path, 'w'))

    def _make_short_locate_data(self):
        data = pd.DataFrame()
        data['Security'] = ['A', 'B', 'C', 'X']
        data['Rate %'] = [-7.5, .95, -3.0, -20.5]
        data['Rqst Qty'] = [1000, 1000, 1000, 1000]
        data['Approv Qty'] = [0, 1000, 1000, 0]
        data['Confirmation'] = ['conftxt'] * 4
        data['Status'] = ['Rejected', 'Approved', 'Approved', 'Rejected']
        # Write to archive
        dt_str = '{d.month}.{d.day}.{d:%y}'.format(d=self.today)
        file_name = 'Roundabout {}.xlsx'.format(dt_str)
        path = os.path.join(self.data_dir, 'StatArbStrategy', 'archive',
                            'locates')
        data.to_excel(os.path.join(path, file_name), index=None)

    def _make_short_sell_kill_list(self):
        data = pd.DataFrame()
        data['Security'] = ['A', 'X']
        data['rate'] = [-7.5, -20.5]
        data['req_qty'] = [1000, 1000]
        data['apr_qty'] = [0, 0]
        data['Status'] = ['Rejected', 'Rejected']
        data['SecCode'] = [101, None]

        # Write to imp dir
        path = os.path.join(self.data_dir, 'short_sell_kill_list.csv')
        data.to_csv(path, index=None)
