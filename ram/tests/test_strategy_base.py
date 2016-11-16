import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.base import Strategy


class TestStrategy(Strategy):

    def run_index(self, index):
        start = '2016-01-{0:02d}'.format(index+1)
        df = pd.DataFrame({
            'V1': [index]},
            index=pd.date_range(start=start, periods=1))
        return df

    def get_iter_index(self):
        return range(4)


class TestStrategyBase(unittest.TestCase):

    def setUp(self):
        self.outdir = os.path.join(os.getenv('GITHUB'), 'ram',
                                   'ram', 'tests')
        self.strategy = TestStrategy(self.outdir)

    def test_run_index(self):
        result = self.strategy.run_index(0)
        benchmark = pd.DataFrame({
            'V1': [0]},
            index=pd.date_range(start='2016-01-{0:02d}'.format(1),
                                periods=1))
        assert_frame_equal(result, benchmark)

    def Xtest_start(self):
        self.strategy.start()
        benchmark = pd.DataFrame({
            'V1': [0]},
            index=pd.date_range(start='2016-01-{0:02d}'.format(1),
                                periods=1))
        assert_frame_equal(result, benchmark)

    def test_run_index_writer(self):
        self.strategy.run_index_writer(0)
        self.strategy.run_index_writer(1)
        result = os.listdir(os.path.join(self.outdir, 'output_TestStrategy'))
        benchmark = ['result_00000.csv', 'result_00001.csv']
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        shutil.rmtree(self.outdir+'/output_TestStrategy')


if __name__ == '__main__':
    unittest.main()
