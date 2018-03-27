import json
import unittest
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.statarb.version_002.constructor.sizes import *


class TestSizeContainer(unittest.TestCase):

    def setUp(self):
        pass

    def test_update_sizes_get_sizes(self):
        size_container = SizeContainer(n_days=2)
        sizes = {'A': 100, 'B': 300, 'C': -1000}
        t = dt.date(2010, 1, 1)
        size_container.update_sizes(sizes, t)
        result = size_container.get_sizes()
        benchmark = {'A': 100.0, 'B': 300.0, 'C': -1000.0}
        self.assertDictEqual(result, benchmark)
        sizes = {'A': 200, 'B': 400, 'C': -500}
        t = dt.date(2010, 1, 2)
        size_container.update_sizes(sizes, t)
        result = size_container.get_sizes()
        benchmark = {'A': 300.0, 'B': 700.0, 'C': -1500.0}
        self.assertDictEqual(result, benchmark)
        sizes = {'A': 200, 'B': 400, 'C': -500}
        t = dt.date(2010, 1, 3)
        size_container.update_sizes(sizes, t)
        result = size_container.get_sizes()
        benchmark = {'A': 400.0, 'B': 800.0, 'C': -1000.0}
        self.assertDictEqual(result, benchmark)

    def test_to_from_json(self):
        size_container = SizeContainer(n_days=2)
        sizes = {'A': 100, 'B': 300, 'C': -1000}
        t = dt.date(2010, 1, 1)
        size_container.update_sizes(sizes, t)
        sizes = {'A': 100, 'B': 100, 'D': -1000}
        t = dt.date(2010, 1, 2)
        size_container.update_sizes(sizes, t)
        sizes = {'A': 100, 'B': 100, 'D': -1000}
        t = dt.date(2010, 1, 3)
        size_container.update_sizes(sizes, t)
        result = json.dumps(size_container.to_json())
        size_container2 = SizeContainer(10)
        size_container2.from_json(json.loads(result))
        self.assertDictEqual(size_container.to_json(),
                             size_container2.to_json())
        self.assertListEqual(size_container2.dates, [dt.date(2010, 1, 2),
                                                     dt.date(2010, 1, 3)])
        self.assertEqual(size_container2.n_days, 2)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
