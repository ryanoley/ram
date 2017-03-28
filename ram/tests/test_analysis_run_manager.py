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

from ram.analysis.run_manager import RunManager


class TestRunManager(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
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

    def test_create_return_frame(self):
        rm1 = RunManager('TestStrategy', 'run_0001')
        rm1.create_return_frame(self.base_path)
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

    def tearDown(self):
        shutil.rmtree(self.base_path)


if __name__ == '__main__':
    unittest.main()
