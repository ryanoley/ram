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
        data = DataContainerPairs()
        signals = SignalModel1()
        signals.set_features(['IBES_asdf'])
        signals._process_args()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
