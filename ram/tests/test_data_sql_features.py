import unittest

import ram.data.sql_features as sqlf


class TestSqlFeatures(unittest.TestCase):

    def setUp(self):
        pass

    def test_sqlcmd_from_feature_list(self):
        features = ['AvgDolVol', 'LAG1_RClose', 'LEAD1_PRMA10_High']
        result = sqlf.sqlcmd_from_feature_list(features)

    def test_parse_input_var(self):
        result = sqlf.parse_input_var('LAG1_ROpen')
        benchmark = {'name': 'LAG1_ROpen', 'datacol': 'Open_',
                     'manip': ('LAG', 1), 'var': ('pass_through_var', 0)}
        self.assertDictEqual(result, benchmark)
        result = sqlf.parse_input_var('LEAD1_PRMA20_Open')
        benchmark = {'name': 'LEAD1_PRMA20_Open', 'datacol': 'AdjOpen',
                     'manip': ('LEAD', 1), 'var': ('PRMA', 20)}
        self.assertDictEqual(result, benchmark)
        result = sqlf.parse_input_var('AvgDolVol')
        benchmark = {'name': 'AvgDolVol', 'datacol': 'AvgDolVol',
                     'var': ('pass_through_var', 0),
                     'manip': ('pass_through_manip', 0)}
        self.assertDictEqual(result, benchmark)
        # Test that these fail
        self.assertRaises(Exception, sqlf.parse_input_var, 'XERB')
        self.assertRaises(Exception, sqlf.parse_input_var, 'Lag1_Open')
        self.assertRaises(Exception, sqlf.parse_input_var, 'OPEN')

    def test_make_cmds(self):
        vstring = 'LAG1_PRMA10_Close'
        result = sqlf.make_cmds(vstring)
        self.assertEqual(
            result[0],
            ' AdjClose / avg(AdjClose) over ( partition by IdcCode '
            'order by Date_ rows between 9 preceding and current row) '
            'as LAG1_PRMA10_Close ')
        self.assertEqual(result[1], ' LAG1_PRMA10_Close ')
        self.assertEqual(
            result[2],
            ' lag(LAG1_PRMA10_Close, 1) over ( partition by IdcCode '
            'order by Date_) as LAG1_PRMA10_Close ')

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
