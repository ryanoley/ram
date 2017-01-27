import unittest
import datetime as dt

from ram.data.sql_features2 import *


class TestSqlFeatures(unittest.TestCase):

    def setUp(self):
        pass

    def test_sqlcmd_from_feature_list(self):
        start_date = dt.datetime(2011, 1, 1)
        end_date = dt.datetime(2012, 1, 1)
        ids = ['AAPL']
        features = ['LAG1_Close', 'LAG1_MA10_Close']
        import pdb; pdb.set_trace()
        result = sqlcmd_from_feature_list(features, ids, start_date, end_date)

    def test_make_commands(self):
        feature_data = [parse_input_var('LAG1_Close'),
                        parse_input_var('LAG1_MA10_Close')]
        result = make_commands(feature_data)

    def test_parse_input_var(self):
        result = parse_input_var('LAG1_ROpen')
        benchmark = {
            'shift': ('LAG', 1),
            'sqlcmd': DATACOL('Open_', 'LAG1_ROpen', None),
            'feature_name': 'LAG1_ROpen',
            'rank': False
        }
        self.assertDictEqual(result, benchmark)

    def test_DATACOL(self):
        result = DATACOL('Open_', 'LAG1_ROpen', None)
        benchmark = "select SecCode, Date_, Open_ as LAG1_ROpen " + \
                    "from ram.dbo.ram_master_equities"
        self.assertEqual(result, benchmark)

    def test_MA(self):
        result = MA('AdjClose', 'PRMA10_Close', 10)
        benchmark = "select SecCode, Date_, avg(AdjClose) over ( " + \
                    "partition by SecCode order by Date_ rows between " + \
                    "9 preceding and current row) as PRMA10_Close from " + \
                    "ram.dbo.ram_master_equities"
        self.assertEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
