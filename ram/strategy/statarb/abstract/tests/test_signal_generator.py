import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator



class SignalGenerator(BaseSignalGenerator):

    def get_args(self):
        return None

    def get_skl_model(self):
        return None

    def generate_signals(self, data_container, **kwargs):
        return None


class TestBaseSignalGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def test_filter_seccodes(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
