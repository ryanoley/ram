import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.implementation.preprocess_new_models import *


class TestPreprocessNewModels(unittest.TestCase):

    def setUp(self):
        # Directory setup
        self.test_data_dir = os.path.join(os.getenv('GITHUB'), 'ram',
                                          'ram', 'test_data')
        # DELETE LEFTOVER DIRECTORIES
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
        os.mkdir(self.test_data_dir)
        path = os.path.join(self.test_data_dir, 'StatArbStrategy')
        os.mkdir(path)
        path = os.path.join(path, 'version_0101')
        os.mkdir(path)
        # Training data - Write two versions plus market data
        data = pd.DataFrame()
        data['Date'] = ['2010-01-01', '2010-01-02', '2010-01-03'] * 2
        data['SecCode'] = ['A'] * 3 + ['B'] * 3
        data['AdjClose'] = range(6)
        data.to_csv(os.path.join(path, '20100101_data.csv'), index=False)
        data['SecCode'] = ['C'] * 3 + ['D'] * 3
        data.to_csv(os.path.join(path, '20100201_data.csv'), index=False)
        data.to_csv(os.path.join(path, 'market_index_data.csv'), index=False)

    def test_get_univ_seccodes(self):
        result = get_univ_seccodes('version_0101',
                                   prepped_data_dir=self.test_data_dir)
        benchmark = ['C', 'D']
        self.assertListEqual(result, benchmark)

    def test_check_implementation_folder_structure(self):
        check_implementation_folder_structure(self.test_data_dir)
        check_implementation_folder_structure(self.test_data_dir)
        result = os.listdir(os.path.join(
            self.test_data_dir, 'StatArbStrategy', 'preprocessed_data'))
        result.sort()
        benchmark = ['preprocess_001', 'preprocess_002']
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)


if __name__ == '__main__':
    unittest.main()
