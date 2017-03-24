import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.base2 import Strategy2


class TestStrategy(Strategy2):

    def run_index(self, index):
        df = pd.DataFrame({
            'V1': [1, 2, 3]},
            index=pd.date_range(start='2016-01-01', periods=3))
        return df

    def get_column_parameters(self):
        return []


class TestStrategyBase2(unittest.TestCase):

    def setUp(self):
        self.prepped_data_dir = os.path.join(
            os.getenv('GITHUB'), 'ram', 'ram', 'tests', 'prepped_data')
        strategy_dir = os.path.join(self.prepped_data_dir, 'TestStrategy')
        version_dir = os.path.join(strategy_dir, 'version_001')
        if os.path.exists(self.prepped_data_dir):
            shutil.rmtree(self.prepped_data_dir)
        # Make some fake data
        os.mkdir(self.prepped_data_dir)
        os.mkdir(strategy_dir)
        os.mkdir(version_dir)
        data = pd.DataFrame({'Date': [1, 2, 3], 'V1': [3, 4, 5]})
        data.to_csv(os.path.join(version_dir, '20100101_data.csv'))
        data.to_csv(os.path.join(version_dir, '20100201_data.csv'))
        self.strategy = TestStrategy('version_001', False)
        self.strategy.register_prepped_data_dir('version_001',
                                                self.prepped_data_dir)
        self.strategy.register_output_dir(self.prepped_data_dir)

    def test_data_files(self):
        benchmark = ['20100101_data.csv', '20100201_data.csv']
        self.assertListEqual(self.strategy._data_files, benchmark)

    def test_get_git_branch_commit(self):
        result = self.strategy._get_git_branch_commit()
        self.assertEquals(len(result), 2)

    def test_read_data_from_index(self):
        import pdb; pdb.set_trace()
        result = self.strategy.read_data_from_index(1)

    def Xtest_run_index_writer(self):
        self.strategy.run_index_writer(0)
        self.strategy.run_index_writer(1)
        #result = os.listdir(self.strategy.strategy_output_dir)
        #benchmark = ['result_00000.csv', 'result_00001.csv']
        #self.assertListEqual(result, benchmark)

    def tearDown(self):
        shutil.rmtree(self.prepped_data_dir)


if __name__ == '__main__':
    unittest.main()
