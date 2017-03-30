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
from ram.analysis.run_aggregator import *


class TestRunAggregator(unittest.TestCase):

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

    def test_add_run(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_return_frame(path=self.base_path)
        ra1 = RunAggregator()
        self.assertEqual(len(ra1.runs), 0)
        ra1.add_run(rm1)
        self.assertEqual(len(ra1.runs), 1)

    def test_aggregate_returns(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.import_return_frame(path=self.base_path)
        # Fake a new strategy by just changing the class name
        rm2 = RunManager('TestStrategy', 'run_0001')
        rm2.import_return_frame(path=self.base_path)
        rm2.strategy_class = 'TestStrategy2'
        ra1 = RunAggregator()
        ra1.add_run(rm1)
        ra1.add_run(rm2)
        results = ra1.aggregate_returns()
        self.assertEqual(results.shape[0], 10)
        self.assertEqual(results.shape[1], 4)
        benchmark = ['TestStrategy_run_0001_0',
                     'TestStrategy_run_0001_1',
                     'TestStrategy2_run_0001_0',
                     'TestStrategy2_run_0001_1']
        self.assertListEqual(results.columns.tolist(), benchmark)

    def tearDown(self):
        shutil.rmtree(self.base_path)


if __name__ == '__main__':
    unittest.main()
