import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_array_equal

from ram.analysis.model_selection.combo_search import CombinationSearch
from ram.analysis.run_manager import RunManager


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
            'strategy_code_version': 'version_0202',
            'description': 'run1'
        }
        self.run2.meta = {
            'prepped_data_version': 'version_0002',
            'strategy_code_version': 'version_0202',
            'description': 'run2'
        }
        # Output dir
        self.output_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'combo_search')

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
