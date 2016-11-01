import unittest
import numpy as np

import ram.data.sql_features as sqlf


class TestSqlFeatures(unittest.TestCase):

    def setUp(self):
        pass

    def test_rolling_avg(self):
        result = sqlf._rolling_avg(10)
        benchmark = ' (avg(Close_ * DividendFactor * SplitFactor) over ' + \
                    '(partition by IdcCode order by Date_ rows between ' + \
                    '9 preceding and current row)) '
        self.assertEqual(result, benchmark)

    def test_adj(self):
        result = sqlf._adj('ADJClose_')
        benchmark = ' (Close_ * DividendFactor * SplitFactor) as ADJClose_ '
        self.assertEqual(result, benchmark)

    def test_prma(self):
        result = sqlf._prma('PRMA10')
        benchmark = '(Close_ * DividendFactor * SplitFactor) / ' + \
                    '(avg(Close_ * DividendFactor * SplitFactor) over ' + \
                    '(partition by IdcCode order by Date_ rows between 9 ' + \
                    'preceding and current row)) as PRMA10'
        self.assertEqual(result, benchmark)
        #
        result = sqlf._prma('PRMA39')
        benchmark = '(Close_ * DividendFactor * SplitFactor) / ' + \
                    '(avg(Close_ * DividendFactor * SplitFactor) over ' + \
                    '(partition by IdcCode order by Date_ rows between 38 ' + \
                    'preceding and current row)) as PRMA39'
        self.assertEqual(result, benchmark)

    def test_bollinger(self):
        result = sqlf._bollinger('BOLL20')
        benchmark = \
            '( (Close_ * DividendFactor * SplitFactor) - ' + \
            '( (avg(Close_ * DividendFactor * SplitFactor) over ' + \
            '(partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) - 2 * (stdev(Close_ * ' + \
            'DividendFactor * SplitFactor) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ')) / nullif((( (avg(Close_ * DividendFactor * SplitFactor) ' + \
            'over (partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) + 2 * (stdev(Close_ * ' + \
            'DividendFactor * SplitFactor) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ') - ( (avg(Close_ * DividendFactor * SplitFactor) over ' + \
            '(partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) - 2 * (stdev(Close_ * ' + \
            'DividendFactor * SplitFactor) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ')), 0) as BOLL20'
        self.assertEqual(result, benchmark)

    def test_sqlcmd_from_feature_list(self):
        features = ['PRMA10', 'BOLL20', 'Close_']
        result = sqlf.sqlcmd_from_feature_list(features)
        # No test written
        features = ['PRMA10', 'Close_', 'AS@*%&$@']
        result = sqlf.sqlcmd_from_feature_list(features)
        self.assertListEqual(result[0], ['PRMA10', 'Close_'])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
