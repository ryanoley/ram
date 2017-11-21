import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.implementation.live.get_data import *


class TestImplementationDailyDataPull(unittest.TestCase):

    def _delete_path(self):
        path = os.path.join(self.data_dir, 'LongPeadStrategy')
        if os.path.isdir(path):
            shutil.rmtree(path)

    def setUp(self):
        self.data_dir = os.path.dirname(os.path.abspath(__file__))
        self._delete_path()

    def Xtest_write_sector_data(self):
        #import pdb; pdb.set_trace()
        cons = ImplementationDailyDataPull(imp_data_dir=self.data_dir)
        data = pd.DataFrame({'V1': range(10)})
        cons.write_sector_data(data, '10')
        cons.write_sector_data(data, '10')
        cons.write_sector_data(data, '30')
        path = os.path.join(
            self.data_dir, 'LongPeadStrategy', 'daily',
            'raw_data_sector_10.csv')
        assert os.path.isfile(path)
        stamp = dt.datetime.now().strftime('%Y%m%d')
        path = os.path.join(
            self.data_dir, 'LongPeadStrategy', 'daily', 'archive',
            'raw_data_sector_10_moved_{}.csv'.format(stamp))
        assert os.path.isfile(path)
        path = os.path.join(self.data_dir,
                            'LongPeadStrategy',
                            'daily',
                            'raw_data_sector_30.csv')
        assert os.path.isfile(path)

    def tearDown(self):
        self._delete_path()


if __name__ == '__main__':
    unittest.main()
