import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_array_equal

from ram.analysis.run_manager import RunManager
from ram.analysis.combo_search import CombinationSearch


class TestCombinationSearch(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1) + dt.timedelta(days=i)
                 for i in range(150)]
        data1 = pd.DataFrame(np.random.randn(150, 12),
                             index=dates)
        data2 = pd.DataFrame(np.random.randn(150, 10),
                             index=dates)
        self.run1 = RunManager('TestStrat', 'run_0001')
        self.run2 = RunManager('TestStrat', 'run_0002')
        self.run1.returns = data1
        self.run2.returns = data2
        self.run1.column_params = \
            {str(i): {'V1': 1, 'V2': 2} for i in range(12)}
        self.run2.column_params = \
            {str(i): {'V1': 1, 'V2': 2} for i in range(10)}
        self.run1.meta = {
            'prepped_data_version': 'version_0001',
            'description': 'run1'
        }
        self.run2.meta = {
            'prepped_data_version': 'version_0002',
            'description': 'run2'
        }
        # Output dir
        self.output_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'combo_search')

    def test_start(self):
        comb = CombinationSearch(write_flag=True,
                                 combo_search_output_dir=self.output_dir,
                                 checkpoint_n_epochs=1)
        comb.params['n_periods'] = 3
        comb.add_run(self.run1)
        comb.add_run(self.run2)
        comb.start(criteria='sharpe')
        comb = CombinationSearch(write_flag=True,
                                 combo_search_output_dir=self.output_dir,
                                 checkpoint_n_epochs=1)
        comb.params['n_periods'] = 3
        comb.add_run(self.run1)
        comb.add_run(self.run2)
        comb.start(criteria='sharpe')
        result = os.listdir(self.output_dir)
        result.sort()
        benchmark = ['combo_run_0001', 'combo_run_0002']
        self.assertEqual(result, benchmark)

    def test_create_training_indexes(self):
        comb = CombinationSearch()
        comb.params['n_periods'] = 2
        comb.add_run(self.run1)
        comb.runs.aggregate_returns()
        returns = comb.runs.returns
        comb._create_training_indexes(returns)
        result = comb._time_indexes
        benchmark = [(0, 59, 90), (31, 90, 120), (59, 120, 150)]
        self.assertListEqual(result, benchmark)
        comb.params['n_periods'] = 1
        comb._create_training_indexes(returns)
        result = comb._time_indexes
        benchmark = [(0, 31, 59), (31, 59, 90), (59, 90, 120), (90, 120, 150)]
        self.assertListEqual(result, benchmark)

    def test_process_results(self):
        dates = [dt.datetime(2015, 1, i) for i in range(1, 4)]
        data1 = pd.DataFrame({
            'V1': [1, 2, 3],
            'V2': [2, 3, 4]
        }, index=dates)
        dates = [dt.datetime(2015, 1, i) for i in range(3, 6)]
        data2 = pd.DataFrame({
            'V2': [1, 2, 3],
            'V3': [2, 3, 4]
        }, index=dates)
        run1 = RunManager('TestStrat', 'run_0001')
        run2 = RunManager('TestStrat', 'run_0002')
        run1.returns = data1
        run2.returns = data2
        run1.column_params = {'V1': {'x': 2}, 'V2': {'x': 4}}
        run2.column_params = {'V2': {'x': 2}, 'V3': {'x': 4}}
        run1.meta = {
            'prepped_data_version': 'version_0001',
            'description': 'Test'
        }
        run2.meta = {
            'prepped_data_version': 'version_0002',
            'description': 'Test2'
        }
        comb = CombinationSearch()
        comb.params['n_periods'] = 3
        comb.params['n_best_ports'] = 1
        comb.add_run(run1)
        comb.add_run(run2)
        comb.runs.aggregate_returns()
        returns = comb.runs.returns
        comb._create_results_objects(returns)
        dates = [dt.datetime(2015, 1, i) for i in range(1, 3)]
        test_rets = pd.DataFrame({0: [1, 2.]}, index=dates)
        scores = np.array([5])
        combs = np.array([(1, 2)])
        comb._process_results(999, test_rets, scores, combs)
        dates = [dt.datetime(2015, 1, i) for i in range(1, 6)]
        benchmark = pd.DataFrame({0: [1, 2, np.nan, np.nan, np.nan]},
                                 index=dates)
        assert_frame_equal(comb.best_results_rets, benchmark)
        assert_array_equal(comb.best_results_combs[999], np.array([[1, 2]]))
        assert_array_equal(comb.best_results_scores[999], np.array([5]))
        # Update shit
        dates = [dt.datetime(2015, 1, i) for i in range(1, 3)]
        test_rets = pd.DataFrame({0: [8, 9]}, index=dates)
        scores = np.array([10])
        combs = np.array([(3, 2)])
        comb._process_results(999, test_rets, scores, combs)
        benchmark.loc[:, 0] = [8, 9, np.nan, np.nan, np.nan]
        assert_frame_equal(comb.best_results_rets, benchmark)
        assert_array_equal(comb.best_results_combs[999], np.array([[3, 2]]))
        assert_array_equal(comb.best_results_scores[999], np.array([10]))

    def test_get_sharpes(self):
        df = pd.DataFrame({0: range(5), 1: range(1, 6), 2: range(2, 7)})
        combs = [(0, 1), (0, 2), (1, 2)]
        comb = CombinationSearch()
        results = comb._get_sharpes(df, combs)
        benchmark = np.array([1.76776695, 2.12132034, 2.47487373])
        assert_array_equal(results.round(5), benchmark.round(5))

    def tearDown(self):
        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)


if __name__ == '__main__':
    unittest.main()
