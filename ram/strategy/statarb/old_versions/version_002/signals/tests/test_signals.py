import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.version_002.signals.signals import *


class TestSignalModel(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_process_args(self):
        signals = SignalModel()
        signals.set_args()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
