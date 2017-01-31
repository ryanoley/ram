import unittest
import datetime as dt

from ram.data.sql_features import *


class TestSqlFeatures(unittest.TestCase):

    def setUp(self):
        pass

    def test_sqlcmd_from_feature_list(self):
        start_date = dt.datetime(2011, 1, 1)
        end_date = dt.datetime(2012, 1, 1)
        ids = ['AAPL']
        features = ['LAG1_RClose', 'LAG1_MA10_Close', 'LAG1_RANK_PRMA10_Close']
        result = sqlcmd_from_feature_list(features, ids, start_date, end_date)

    def test_make_commands(self):
        feature_data = [parse_input_var('LEAD2_RClose', 'ABC', ''),
                        parse_input_var('LAG1_RANK_MA10_Close', 'ABC', '')]
        result = make_commands(feature_data)
        benchmark = ', LEAD(x0.LEAD2_RClose, 2) over ( partition by ' + \
                    'x0.SecCode order by x0.Date_) as LEAD2_RClose , RANK(' + \
                    'LAG(x1.LAG1_RANK_MA10_Close, 1) over ( partition ' + \
                    'by x1.SecCode order by x1.Date_)) over ( partition ' + \
                    'by SecCode order by Date_) as LAG1_RANK_MA10_Close'
        self.assertEqual(result[0], benchmark)
        benchmark = 'left join (select SecCode, Date_, Close_ ' + \
                    'as LEAD2_RClose from ABC A) x0 on A.SecCode = ' + \
                    'x0.SecCode and A.Date_ = x0.Date_ left join (select ' + \
                    'SecCode, Date_, avg(AdjClose) over ( partition by ' + \
                    'SecCode order by Date_ rows between 9 preceding and ' + \
                    'current row) as LAG1_RANK_MA10_Close from ABC A) x1 ' + \
                    'on A.SecCode = x1.SecCode and A.Date_ = x1.Date_'
        self.assertEqual(result[1], benchmark)

    def test_parse_input_var(self):
        result = parse_input_var('LAG1_ROpen', 'ABC', '')
        benchmark = {
            'shift': ('LAG', 1),
            'sqlcmd': DATACOL('Open_', 'LAG1_ROpen', None, 'ABC'),
            'feature_name': 'LAG1_ROpen',
            'rank': False
        }
        self.assertDictEqual(result, benchmark)
        result = parse_input_var('LEAD2_RANK_PRMA10_Close', 'ABC', '')
        benchmark = {
            'shift': ('LEAD', 2),
            'sqlcmd': PRMA('AdjClose', 'LEAD2_RANK_PRMA10_Close', 10, 'ABC'),
            'feature_name': 'LEAD2_RANK_PRMA10_Close',
            'rank': True
        }
        self.assertDictEqual(result, benchmark)

    def test_DATACOL(self):
        result = DATACOL('Open_', 'LAG1_ROpen', None, 'ABC')
        benchmark = "select SecCode, Date_, Open_ as LAG1_ROpen from ABC A"
        self.assertEqual(result, benchmark)

    def test_MA(self):
        result = MA('AdjClose', 'PRMA10_Close', 10, 'ABC')
        benchmark = "select SecCode, Date_, avg(AdjClose) over ( " + \
                    "partition by SecCode order by Date_ rows between " + \
                    "9 preceding and current row) as PRMA10_Close from ABC A"
        self.assertEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
