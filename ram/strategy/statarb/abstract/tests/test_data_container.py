import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.abstract.data_container import BaseDataContainer



class DataContainer(BaseDataContainer):

    def get_args(self):
        return {'v1': [1, 2]}

    def add_data(self, data, time_index):
        return None

    def prep_data(self, time_index, **kwargs):
        return None



class TestBaseDataContainer(unittest.TestCase):

    def setUp(self):
        pass

    def test_filter_seccodes(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
