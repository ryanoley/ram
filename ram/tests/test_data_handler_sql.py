
import unittest
import numpy as np
import pandas as pd
import datetime as dt
from numpy.testing import assert_array_equal

from ram.data.data_handler_sql import DataHandlerSQL
from ram.data.data_handler_sql import _check_for_duplicates


class TestDataHandlerSQL(unittest.TestCase):

    def setUp(self):
        self.dh = DataHandlerSQL()
        assert self.dh._connection.connected == 1

    def test_prior_trading_date(self):
        result = self.dh.prior_trading_date('2017-03-01')
        benchmark = dt.date(2017, 2, 28)
        self.assertEqual(result, benchmark)
        result = self.dh.prior_trading_date(['2017-03-01', '2017-07-15',
                                             '2017-01-01'])
        benchmark = [dt.date(2017, 2, 28), dt.date(2017, 7, 14),
                     dt.date(2016, 12, 30)]
        assert_array_equal(result, benchmark)
        result = self.dh.prior_trading_date('notadate')
        self.assertTrue(np.isnan(result))

    def test_map_ticker_to_id(self):
        result = self.dh._map_ticker_to_id('SPY')
        self.assertEqual(result.SecCode.iloc[0], 61494)
        self.assertEqual(result.Ticker.iloc[0], 'SPY')

    def test_check_for_duplicates(self):
        test_df = pd.DataFrame(data={
            'SecCode': [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
            'Date':  pd.date_range('1900-01-01', '1900-01-12'),
            'Close': range(1, 13)})
        result = _check_for_duplicates(test_df, 'Date')
        self.assertEqual(result, None)
        result = _check_for_duplicates(test_df, ['SecCode', 'Date'])
        self.assertEqual(result, None)
        test_df.loc[7:, 'Date'] = pd.Timestamp('19000108')
        self.assertRaises(ValueError, _check_for_duplicates, test_df,
                          ['SecCode', 'Date'])

    def tearDown(self):
        self.dh.close_connections()


if __name__ == '__main__':
    unittest.main()
