import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.long_pead.implementation.get_data import *


class TestImplementationData(unittest.TestCase):

    def setUp(self):
        pass

    def test_constructor(self):
        cons = ImplementationMorningDataPull()
        #cons.morning_data_pull(False)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
