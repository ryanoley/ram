import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.implementation.get_data import *


class TestImplementationTrainingDataPull(unittest.TestCase):

    def _delete_path(self):
        path = os.path.join(self.data_dir, 'LongPeadStrategy')
        if os.path.isdir(path):
            shutil.rmtree(path)

    def setUp(self):
        self.data_dir = os.path.dirname(os.path.abspath(__file__))
        self._delete_path()

    def test_write_sector_data(self):
        cons = ImplementationTrainingDataPull(imp_data_dir=self.data_dir)
        data = pd.DataFrame({'V1': range(10)})
        cons.write_sector_data(data, '10')
        cons.write_sector_data(data, '10')
        cons.write_sector_data(data, '10')
        path = os.path.join(self.data_dir,
                            'LongPeadStrategy',
                            'training',
                            'raw_sector_10.csv')
        assert os.path.isfile(path)

    def test_get_ids_query_dates(self):
        cons = ImplementationTrainingDataPull(imp_data_dir=self.data_dir)
        result = cons.get_ids_query_dates()
        self.assertEqual(len(result), 13)
        self.assertIsInstance(result[1], dt.date)

    def tearDown(self):
        self._delete_path()


if __name__ == '__main__':
    unittest.main()
