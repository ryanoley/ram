import pymongo
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.data.dh_mongo import DataHandlerMongoDb


class TestDataHandlerMongoDb(unittest.TestCase):

    def setUp(self):
        # Make Mongo collection
        ids = ['_a'] * 5 + ['_b'] * 4
        dates = [dt.datetime(2016, 2, 1, 0, 0) + dt.timedelta(days=i)
                 for i in range(5)]
        dates2 = dates * 2
        dates2.pop(7)
        p_close = range(1, 6) + [10, 14, 3, 4]
        dolvol = [10] * 5 + [3] * 4
        oyf = ['T'] * 5 + ['F'] * 4
        # Create some data
        df = pd.DataFrame({'DailyDate': dates2, 'ID': ids, 'Close': p_close,
                           'AvgDolVolume': dolvol, 'OneYearFlag': oyf,
                           'NormalTradingFlag': oyf,
                           'AdjustFactor': [2]*len(dates2),
                           'SplitFactor': [1]*len(dates2)})
        posts = [x for x in df.T.to_dict().itervalues()]
        client = pymongo.MongoClient()
        if 'ramsystem_test' in client.database_names():
            client.drop_database('ramsystem_test')
        mdb = client.ramsystem_test.prices
        mdb.insert_many(posts)
        # Create meta data
        df = pd.DataFrame({'ID': ['_a', '_b'], 'Sector': [10, 15],
                           'CUSIP': [123, 456]})
        posts = [x for x in df.T.to_dict().itervalues()]
        client = pymongo.MongoClient()
        mdb = client.ramsystem_test.meta
        mdb.insert_many(posts)
        client.close()

    def test_make_data_all_filters(self):
        dh = DataHandlerMongoDb(host='localhost',
                                db='ramsystem_test',
                                collection='prices')
        result = dh.get_filtered_univ_data(
            univ_size=2,
            features='Close',
            start_date=dt.datetime(2016, 2, 2),
            filter_date=dt.datetime(2016, 2, 4),
            end_date=dt.datetime(2016, 2, 4),
            filter_column='AvgDolVolume',
            filter_bad_ids=False)
        benchmark = pd.DataFrame({
            'Close': [2, 3, 4, 14, 3],
            'ID': ['a']*3 + ['b']*2,
            'Date': [dt.datetime(2016, 2, 2), dt.datetime(2016, 2, 3),
                     dt.datetime(2016, 2, 4), dt.datetime(2016, 2, 2),
                     dt.datetime(2016, 2, 4)]})
        benchmark = benchmark[['Date', 'ID', 'Close']]
        assert_frame_equal(result, benchmark)
        # Test 2
        result = dh.get_filtered_univ_data(
            univ_size=1,
            features='Close',
            start_date=dt.datetime(2016, 2, 2),
            filter_date=dt.datetime(2016, 2, 4),
            end_date=dt.datetime(2016, 2, 4),
            filter_column='AvgDolVolume',
            filter_bad_ids=True)
        benchmark = pd.DataFrame({
            'Close': [2, 3, 4],
            'ID': ['a']*3,
            'Date': [dt.datetime(2016, 2, 2), dt.datetime(2016, 2, 3),
                     dt.datetime(2016, 2, 4)]})
        benchmark = benchmark[['Date', 'ID', 'Close']]
        assert_frame_equal(result, benchmark)

    def Xtest_make_meta(self):
        client = pymongo.MongoClient()
        mdb = client.statarb_test.prices
        meta = client.statarb_test.meta
        bdh = BaseDataHandlerMongoDb(mdb, meta,
                                     ids='ID', dates='DailyDate')
        bdh.make_meta()
        result = bdh.meta
        self.assertListEqual(result.ID.values.tolist(), ['a', 'b'])
        self.assertListEqual(result.columns.tolist(),
                             ['ID', 'CUSIP', 'Sector'])
        bdh.make_data(['Close'],
                      dt.datetime(2016, 2, 2),
                      dt.datetime(2016, 2, 4),
                      filter_date=dt.datetime(2016, 2, 4),
                      filter_num=1,
                      filter_column='AvgDolVolume',
                      filter_bad_ids=True)
        bdh.make_meta()
        result = bdh.meta
        self.assertListEqual(result.ID.values.tolist(), ['a'])
        client.close()

    def tearDown(self):
        client = pymongo.MongoClient()
        client.drop_database('statarb_test')
        client.close()


if __name__ == '__main__':
    unittest.main()
