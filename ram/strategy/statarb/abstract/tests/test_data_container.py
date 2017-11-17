import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.data.feature_creator import *
from ram.strategy.statarb.abstract.data_container import BaseDataContainer


class DataContainer(BaseDataContainer):

    def get_args(self):
        return {'v1': [1, 2]}

    def add_data(self, data, time_index):
        # Process some data
        open_ = clean_pivot_raw_data(data, 'AdjOpen')
        high = clean_pivot_raw_data(data, 'AdjHigh')
        low = clean_pivot_raw_data(data, 'AdjLow')
        close = clean_pivot_raw_data(data, 'AdjClose')
        volume = clean_pivot_raw_data(data, 'AdjVolume')
        # Create variables
        v1 = PRMA().calculate_all_dates(close, 2)
        v2 = PRMA().calculate_all_dates(close, 4)
        v3 = outlier_rank(v2)

    def add_market_data(self, add_market_data):
        return None

    def prep_data(self, time_index, **kwargs):
        return None


class TestBaseDataContainer(unittest.TestCase):

    def setUp(self):
        pass

    def test_add_data(self):
        df = pd.DataFrame()
        df['SecCode'] = ['a'] * 6 + ['b'] * 6
        df['Date'] = [dt.date(2010, 1, i) for i in range(1, 7)] * 2
        df['AdjHigh'] = range(1, 13)
        df['AdjLow'] = range(1, 13)
        df['AdjClose'] = range(1, 13)
        df['AdjOpen'] = range(1, 13)
        df['AdjVolume'] = range(12)
        data = DataContainer()
        import pdb; pdb.set_trace()
        data.add_data(df, 10)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
