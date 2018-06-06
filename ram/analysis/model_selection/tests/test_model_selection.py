import os
import time
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_array_equal

from ram.analysis.model_selection.model_selection import *
from ram.analysis.run_manager import RunManager


class ModelSelection1(ModelSelection):

    def get_implementation_name(self):
        return 'ModelSelection1'

    def get_top_models(self, time_index, train_data):
        # Combine top 1 and 2, and 3 and 4, and 5 and 6
        inds = np.argsort(-1 * train_data.mean().values)
        output = [list(inds[:2]), list(inds[2:4]), list(inds[4:6])]
        return output, [1, 2, 3]


class TestModelSelection(unittest.TestCase):

    def setUp(self):
        np.random.seed(100)
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
            'strategy_code_version': 'version_0202',
            'description': 'run1',
            'blueprint': {}
        }
        self.run2.meta = {
            'prepped_data_version': 'version_0002',
            'strategy_code_version': 'version_0202',
            'description': 'run2',
            'blueprint': {}
        }
        # Output dir
        self.output_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'model_selection')

    def test_init(self):
        select = ModelSelection1(write_flag=True,
                                 gcp_cloud_implementation=False,
                                 model_selection_output_dir=self.output_dir)
        self.assertTrue(os.path.isdir(self.output_dir))
        self.assertTrue(os.path.isdir(select._output_dir))

    def test_create_training_test_indexes(self):
        dates = pd.DatetimeIndex([dt.date(2015, 1, 1) + dt.timedelta(days=i)
                                  for i in range(18)])
        result = create_training_test_indexes(dates, 'w')
        self.assertEqual(result[0][0][0], pd.Timestamp(2015, 1, 1))
        self.assertEqual(result[0][0][-1], pd.Timestamp(2015, 1, 4))
        self.assertEqual(result[0][1][0], pd.Timestamp(2015, 1, 5))
        self.assertEqual(result[0][1][-1], pd.Timestamp(2015, 1, 11))
        self.assertEqual(result[1][0][0], pd.Timestamp(2015, 1, 1))
        self.assertEqual(result[1][0][-1], pd.Timestamp(2015, 1, 11))
        self.assertEqual(result[1][1][0], pd.Timestamp(2015, 1, 12))
        self.assertEqual(result[1][1][-1], pd.Timestamp(2015, 1, 18))
        result = create_training_test_indexes(dates, 'w', 1)
        self.assertEqual(result[1][0][0], pd.Timestamp(2015, 1, 5))
        self.assertEqual(result[1][0][-1], pd.Timestamp(2015, 1, 11))
        self.assertEqual(result[1][1][0], pd.Timestamp(2015, 1, 12))
        self.assertEqual(result[1][1][-1], pd.Timestamp(2015, 1, 18))

        # HACK THIS IS COMMENTED OUT IN MODEL_SELECTION.PY - GIT ISSUE
        # assert_array_equal(result[2][0], dates)
        # self.assertListEqual(result[2][1], [])

    def test_start(self):
        select = ModelSelection1(write_flag=True,
                                 gcp_cloud_implementation=False,
                                 model_selection_output_dir=self.output_dir)
        select.params['training_periods'] = 2
        select.add_run(self.run1)
        select.add_run(self.run2)
        select.start()

    def test_clean_model_indexes(self):
        model_indexes = [[0, 1], [1, 3], [1, 2]]
        scores = [2, 3, 4]
        original = [[0, 1], [1, 9], [1, 9]]
        result = clean_model_indexes(model_indexes, scores, original)
        benchmark = [[1, 3], [1, 2]]
        self.assertListEqual(result[0], benchmark)
        benchmark = [3, 4]
        self.assertListEqual(result[1], benchmark)

    def test_process_results(self):
        select = ModelSelection1(write_flag=True,
                                 gcp_cloud_implementation=False,
                                 model_selection_output_dir=self.output_dir)
        all_data = self.run1.returns
        select._create_results_objects(all_data)
        time_index = 0
        model_indexes = [[0, 2], [1, 3]]
        scores = [10, 21]
        test_data = all_data.iloc[100:].copy()
        # Fake some of the test data to make clear that columns are selected
        # properly
        test_data.loc[:, model_indexes[0]] = 99
        test_data.loc[:, model_indexes[1]] = 77
        select._process_results(time_index, test_data, model_indexes, scores)
        result = select.best_results_returns
        self.assertEqual(result.iloc[-1, 0], 77)
        self.assertEqual(result.iloc[-1, 1], 99)
        benchmark = {0: [21, 10]}
        self.assertDictEqual(select.best_results_scores, benchmark)
        benchmark = {0: [[1, 3], [0, 2]]}
        self.assertDictEqual(select.best_results_column_indexes, benchmark)
        # Change it up
        model_indexes = [[4, 6], [10, 11]]
        scores = [50, 1]
        test_data = all_data.iloc[100:].copy()
        test_data.loc[:, model_indexes[0]] = 55
        test_data.loc[:, model_indexes[1]] = 44
        select._process_results(time_index, test_data, model_indexes, scores)
        result = select.best_results_returns
        self.assertEqual(result.iloc[-1, 0], 55)
        self.assertEqual(result.iloc[-1, 1], 77)
        benchmark = {0: [50, 21]}
        self.assertDictEqual(select.best_results_scores, benchmark)
        benchmark = {0: [[4, 6], [1, 3]]}
        self.assertDictEqual(select.best_results_column_indexes, benchmark)

    def tearDown(self):
        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)


if __name__ == '__main__':
    unittest.main()
