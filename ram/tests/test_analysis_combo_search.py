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
        self.comb = CombinationSearch()
        self.comb.add_data(data, 'commit_1')

    def test_init(self):
        comb = CombinationSearch()

    def test_add_data_clean_input_data(self):
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
        comb = CombinationSearch()
        comb.add_data(data1, 'commit_1')
        comb.add_data(data2, 'commit_2')
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
        comb = self.comb
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

    def Xtest_start(self):
        comb = self.comb
        import pdb; pdb.set_trace()
        comb.set_training_params(freq='m', n_periods=2,
                                 n_ports_per_combo=5, n_best_combos=2)
        comb.start()

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
        comb = CombinationSearch()
        comb.add_data(data1, 'commit_1')
        comb.add_data(data2, 'commit_2')
        comb._train_n_best_combos = 1
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
        results = self.comb._get_sharpes(df, combs)
        benchmark = np.array([ 1.76776695,  2.12132034,  2.47487373])
        assert_array_equal(results.round(5), benchmark.round(5))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
