import unittest
import numpy as np
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.gvkey_mapping import *


class TestGVKeyMapping(unittest.TestCase):

    def setUp(self):
        pass

    def test_two_gvkeys_one_idccode(self):
        data = pd.DataFrame()
        data['Code'] = [10, 10, 10, 10]
        data['GVKey'] = [1, 1, 2, 2]
        data['GVKeyChangeDate'] = [dt.datetime(2010, 1, 1),
                                   dt.datetime(2010, 2, 2),
                                   dt.datetime(2010, 3, 3),
                                   dt.datetime(2010, 4, 4)]
        result = two_gvkeys_one_idccode(data)
        benchmark = pd.DataFrame()
        benchmark['IdcCode'] = [10, 10]
        benchmark['GVKey'] = [1, 2]
        benchmark['StartDate'] = [dt.datetime(1959, 1, 1),
                                  dt.datetime(2010, 2, 3)]
        benchmark['EndDate'] = [dt.datetime(2010, 2, 2),
                                dt.datetime(2079, 1, 1)]
        assert_frame_equal(result, benchmark)

    def test_one_gvkey_rollup(self):
        data = pd.DataFrame()
        data['Code'] = [1, 1, 2, 2]
        data['GVKey'] = [10, 10, 10, 10]
        data['RamPricingMinDate'] = [dt.datetime(2010, 1, 1),
                                     dt.datetime(2010, 2, 2),
                                     dt.datetime(2010, 3, 3),
                                     dt.datetime(2010, 4, 4)]
        data['RamPricingMaxDate'] = [dt.datetime(2010, 1, 20),
                                     dt.datetime(2010, 2, 20),
                                     dt.datetime(2010, 3, 20),
                                     dt.datetime(2010, 4, 20)]
        result = one_gvkey_rollup(data)
        benchmark = pd.DataFrame()
        benchmark['IdcCode'] = [1, 2]
        benchmark['GVKey'] = [10, 10]
        benchmark['StartDate'] = dt.datetime(1959, 1, 1)
        benchmark['EndDate'] = dt.datetime(2079, 1, 1)
        assert_frame_equal(result, benchmark)

    def test_manually_handle(self):
        data = pd.DataFrame()
        data['Code'] = [1, 1, 2, 2]
        data['GVKey'] = [10, 10, 10, 10]
        data['RamPricingMinDate'] = [dt.datetime(2010, 1, 1),
                                     dt.datetime(2010, 2, 2),
                                     dt.datetime(2010, 3, 3),
                                     dt.datetime(2010, 4, 4)]
        data['RamPricingMaxDate'] = [dt.datetime(2010, 1, 20),
                                     dt.datetime(2010, 2, 20),
                                     dt.datetime(2010, 3, 20),
                                     dt.datetime(2010, 4, 20)]
        #result = manually_handle(data)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
