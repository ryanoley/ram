import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_array_equal

from ram.analysis.combo_search import CombinationSearch


class TestCombinationSearch(unittest.TestCase):

    def setUp(self):
        dates = [dt.datetime(2015, 1, 1) + dt.timedelta(days=i)
                 for i in range(150)]
        data = pd.DataFrame(np.random.randn(150, 50),
                            index=dates)
        self.data = data
        dates = [dt.datetime(2015, 1, i) for i in range(1, 4)]
        self.data1 = pd.DataFrame({
            'V1': [1, 2, 3],
            'V2': [2, 3, 4]
        }, index=dates)
        dates = [dt.datetime(2015, 1, i) for i in range(3, 6)]
        self.data2 = pd.DataFrame({
            'V2': [1, 2, 3],
            'V3': [2, 3, 4]
        }, index=dates)
        # Write to folder
        dir1 = os.path.join(os.getenv('GITHUB'), 'ram', 'ram',
                            'tests', 'simulations')
        self.base_dir = os.path.join(os.getenv('GITHUB'), 'ram', 'ram',
                                     'tests', 'simulations')
        os.mkdir(self.base_dir)
        os.mkdir(os.path.join(self.base_dir, 'StatArbStrategy'))
        os.mkdir(os.path.join(self.base_dir, 'StatArbStrategy', 'run_0001'))
        os.mkdir(os.path.join(self.base_dir, 'StatArbStrategy', 'run_0002'))
        self.data1.to_csv(os.path.join(self.base_dir, 'StatArbStrategy',
                                       'run_0001', 'results.csv'))
        self.data2.to_csv(os.path.join(self.base_dir, 'StatArbStrategy',
                                       'run_0002', 'results.csv'))

    def Xtest_start(self):
        # For dev purposes and not testing due to infinite loop
        comb = CombinationSearch('StatArbStrategy', True)
        comb.simulation_dir = self.base_dir
        comb.start('run_0001')

    def test_add_data_clean_input_data(self):
        comb = CombinationSearch('StatArbStrategy')
        comb._add_data(self.data1, 'commit_1')
        comb._add_data(self.data2, 'commit_2')
        dates = [dt.datetime(2015, 1, i) for i in range(1, 6)]
        benchmark = pd.DataFrame(index=dates)
        benchmark[0] = [1, 2, 3, np.nan, np.nan]
        benchmark[1] = [2, 3, 4, np.nan, np.nan]
        benchmark[2] = [np.nan, np.nan, 1, 2, 3]
        benchmark[3] = [np.nan, np.nan, 2, 3, 4]
        assert_frame_equal(comb.data, benchmark)
        self.assertSetEqual(set(comb.data_labels),
                            set(['commit_2', 'commit_1']))
        self.assertListEqual(comb.data_labels['commit_1'].tolist(),
                             ['V1', 'V2'])
        self.assertListEqual(comb.data_labels['commit_2'].tolist(),
                             ['V2', 'V3'])
        comb._clean_input_data()
        benchmark[0] = [1, 2, 3, 0, 0.]
        benchmark[1] = [2, 3, 4, 0, 0.]
        assert_frame_equal(comb.data, benchmark)

    def test_create_training_indexes(self):
        comb = CombinationSearch('StatArbStrategy')
        comb._add_data(self.data, 'commit_1')
        comb.set_training_params(freq='m', n_periods=2)
        comb._create_training_indexes()
        result = comb._time_indexes
        benchmark = [(0, 59, 90), (31, 90, 120), (59, 120, 150)]
        self.assertListEqual(result, benchmark)
        comb.set_training_params(freq='m', n_periods=-1)
        comb._create_training_indexes()
        result = comb._time_indexes
        benchmark = [(0, 31, 59), (0, 59, 90), (0, 90, 120), (0, 120, 150)]
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
        comb = CombinationSearch('StatArbStrategy')
        comb._add_data(data1, 'commit_1')
        comb._add_data(data2, 'commit_2')
        comb.set_training_params(n_best_combos=1)
        comb._create_results_objects()
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
        comb = CombinationSearch('StatArbStrategy')
        results = comb._get_sharpes(df, combs)
        benchmark = np.array([ 1.76776695,  2.12132034,  2.47487373])
        assert_array_equal(results.round(5), benchmark.round(5))

    def test_write_init_output_load_session(self):
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
        comb = CombinationSearch('StatArbStrategy', True)
        comb.simulation_dir = self.base_dir
        comb._add_data(data1, 'commit_1')
        comb._add_data(data2, 'commit_2')
        comb.set_training_params(freq='m', n_periods=2,
                                 n_ports_per_combo=5, n_best_combos=2)
        comb._create_results_objects()
        comb.params['seed_ind'] = 99
        comb._write_init_output()

        comb2 = CombinationSearch('StatArbStrategy')
        comb2.simulation_dir = self.base_dir
        comb2._load_comb_search_session('combo_0001')

        dates = pd.DatetimeIndex([dt.date(2015, 1, i) for i in range(1, 6)])
        benchmark = pd.DataFrame(index=dates, columns=[0, 1], dtype=np.float_)
        assert_frame_equal(comb2.best_results_rets, benchmark)
        self.assertEqual(comb2.params['seed_ind'], 99)

    def tearDown(self):
        shutil.rmtree(self.base_dir)


if __name__ == '__main__':
    unittest.main()
