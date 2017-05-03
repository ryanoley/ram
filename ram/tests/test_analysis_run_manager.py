import os
import json
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.analysis.run_manager import *


class TestRunManager(unittest.TestCase):

    def setUp(self):
        self.base_path = os.path.join(os.getenv('GITHUB'), 'ram', 'ram',
                                      'tests', 'test_simulations')
        if os.path.isdir(self.base_path):
            shutil.rmtree(self.base_path)
        os.mkdir(self.base_path)
        strategy_path = os.path.join(self.base_path, 'TestStrategy')
        os.mkdir(strategy_path)
        run_path = os.path.join(strategy_path, 'run_0001')
        os.mkdir(run_path)
        results_path = os.path.join(run_path, 'index_outputs')
        os.mkdir(results_path)
        # Create some data
        data = pd.DataFrame()
        data[0] = [1, 2, 3, 4, 5]
        data[1] = [6, 7, 8, 9, 10]
        data.index = ['2010-01-{0:02d}'.format(i) for i in range(1, 6)]
        data.to_csv(os.path.join(results_path, '20100101_returns.csv'))
        data = pd.DataFrame()
        data[0] = [1, 2, 3, 4, 5]
        data[1] = [6, 7, 8, 9, 10]
        data.index = ['2010-01-{0:02d}'.format(i) for i in range(6, 11)]
        data.to_csv(os.path.join(results_path, '20100106_returns.csv'))
        # Create some stats
        stats = {
            0: {'stat1': 10, 'stat2': 20},
            1: {'stat1': 20, 'stat2': 40}}
        with open(os.path.join(results_path, '20100101_stats.json'), 'w') as f:
            json.dump(stats, f)
        f.close()
        stats = {
            0: {'stat1': 55, 'stat2': 75},
            1: {'stat1': 65, 'stat2': 95}}
        with open(os.path.join(results_path, '20100106_stats.json'), 'w') as f:
            json.dump(stats, f)
        f.close()
        # Create a meta file
        meta = {'description': 'Test data', 'start_time': '2010-01-01'}
        with open(os.path.join(run_path, 'meta.json'), 'w') as f:
            json.dump(meta, f)
        f.close()
        # Create column params
        params = {0: {'p1': 10, 'p2': 20}, 1: {'p1': 20, 'p2': 30}}
        with open(os.path.join(run_path, 'column_params.json'), 'w') as f:
            json.dump(params, f)
        f.close()

    def test_import_return_frame(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_return_frame(path=self.base_path)
        data = pd.DataFrame()
        data['0'] = [1, 2, 3, 4, 5.] * 2
        data['1'] = [6, 7, 8, 9, 10.] * 2
        data.index = ['2010-01-{0:02d}'.format(i) for i in range(1, 11)]
        data.index = convert_date_array(data.index)
        assert_frame_equal(rm1.returns, data)

    def test_import_stats(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_stats(self.base_path)

    def test_import_meta(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_meta(self.base_path)
        benchmark = {'start_time': '2010-01-01', 'description': 'Test data'}
        self.assertDictEqual(rm1.meta, benchmark)

    def test_import_column_params(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_column_params(self.base_path)

    def test_analyze_parameters(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_return_frame(path=self.base_path)
        rm1.import_column_params(self.base_path)
        rm1.import_meta(self.base_path)
        rm1.import_stats(self.base_path)
        results = rm1.analyze_parameters()

    def test_classify_params(self):
        result = classify_params({
            '0': {'V1': 1, 'V2': 44, 'V3': 888},
            '1': {'V1': 1, 'V2': 55, 'V3': 999},
            '2': {'V1': 2, 'V2': 44, 'V3': 1111},
            '3': {'V1': 2, 'V2': 44, 'V3': 2222},
            '4': {'V1': 3, 'V2': 55, 'V3': 2222},
            '5': {'V1': 3, 'V2': 55, 'V3': 999},
        })
        benchmark = {
            'V1': {1: ['0', '1'],
                   2: ['2', '3'],
                   3: ['4', '5']},
            'V2': {44: ['0', '2', '3'],
                   55: ['1', '4', '5']},
            'V3': {888: ['0'],
                   999: ['1', '5'],
                   1111: ['2'],
                   2222: ['3', '4']}
        }
        self.assertDictEqual(result, benchmark)

    def test_aggregate_statistics(self):
        stats = {
            '20100101_stats.json': {
                'col1': {
                    'stat1': 0.01,
                    'stat2': 0.02
                },
                'col2': {
                    'stat1': 0.03,
                    'stat2': 0.04
                }
            },
            '20100108_stats.json': {
                'col1': {
                    'stat1': 0.03,
                    'stat2': 0.04
                },
                'col2': {
                    'stat1': 0.05,
                    'stat2': 0.06
                }
            }
        }
        result = aggregate_statistics(stats, 1900)
        # Round results due to float imprecision
        for key in result.keys():
            for stat in result[key].keys():
                x = result[key][stat]
                result[key][stat] = (round(x[0], 2), round(x[1], 2))
        benchmark = {
            'col1':
                {
                    'stat1': (0.02, 0.01),
                    'stat2': (0.03, 0.01)
                },
            'col2':
                {
                    'stat1': (0.04, 0.01),
                    'stat2': (0.05, 0.01)
                },
            }
        self.assertDictEqual(result, benchmark)

    def test_format_param_results(self):
        dates = [dt.datetime(2015, 1, 1),
                 dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3),
                 dt.datetime(2015, 1, 4)]
        data = pd.DataFrame({
            '0': [1, 2, 3, 4],
            '1': [3, 2, 4, 3],
            '2': [99, 2, 3, 4]
        }, index=dates)
        data.index.name = 'Date'
        cparams = {
            'V1': {1: ['0', '2'],
                   2: ['1']},
            'V2': {1: ['0', '1'],
                   2: ['2']}
        }
        astats = {
            '0':
                {
                    'stat1': (0.01, 0.01),
                    'stat2': (0.02, 0.01)
                },
            '1':
                {
                    'stat1': (0.11, 0.01),
                    'stat2': (0.12, 0.01)
                },
            '2':
                {
                    'stat1': (0.21, 0.01),
                    'stat2': (0.22, 0.01)
                },
            }
        result = format_param_results(data, cparams, astats, 2000)
        benchmark = pd.DataFrame(index=range(4))
        benchmark['Param'] = ['V1', 'V1', 'V2', 'V2']
        benchmark['Val'] = [1, 2, 1, 2]
        benchmark['Count'] = [2, 1, 2, 1]
        benchmark['MeanTotalRet'] = [59, 12, 11, 108.]
        benchmark['MeanSharpe'] = [1.249455, 3.674235, 2.805363, 0.562419]
        benchmark['stat1'] = [.11, .11, .06, .21]
        benchmark['stat2'] = [.12, .12, .07, .22]
        assert_frame_equal(result, benchmark)

    def test_get_run_names(self):
        result = RunManager.get_run_names('TestStrategy', self.base_path)
        benchmark = pd.DataFrame(columns=['Run', 'Description'], index=[0])
        benchmark['Run'] = ['run_0001']
        benchmark['Description'] = ['Test data']
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        shutil.rmtree(self.base_path)


if __name__ == '__main__':
    unittest.main()
