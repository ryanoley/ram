import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.version_001.signals.signals1 import *
from ram.strategy.statarb.version_001.data.data_container_pairs import *


class TestSignalModel1(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_process_args(self):
        signals = SignalModel1()
        import pdb; pdb.set_trace()
        signals.set_args()


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
