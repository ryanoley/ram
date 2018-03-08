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

    def set_args(self):
        return None

    def set_features(self, features):
        return None

    def set_train_data(self, train_data):
        return None

    def set_train_responses(self, train_responses):
        return None

    def set_test_data(self, test_data):
        return None

    def fit_model(self):
        return None

    def get_model(self):
        return None

    def set_model(self):
        return None

    def get_signals(self):
        return None


class TestBaseSignalGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
