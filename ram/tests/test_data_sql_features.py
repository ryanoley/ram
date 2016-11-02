import unittest
import numpy as np

import ram.data.sql_features as sqlf


class TestSqlFeatures(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_vol(self):
        import pdb; pdb.set_trace()
        result = sqlf._vol('VOL30')
        benchmark = 'stdev( (Close_ * DividendFactor * SplitFactor) / ' + \
                    '( Lag( (Close_ * DividendFactor * SplitFactor) , 1) ' + \
                    'over (partition by IdcCode order by Date_) ))  over ' + \
                    '(partition by IdcCode order by Date_ rows between 29 ' + \
                    'preceding and current row) as VOL30'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'VOL30')

    def test_lag(self):
        result = sqlf._lag('Close_', 2)
        benchmark = '(Lag(Close_, 2) ' + \
                    'over (partition by IdcCode order by Date_))'
        self.assertEqual(clean(result), clean(benchmark))

    def test_lag_adj_price(self):
        result = sqlf._lag_adj_price('LAGADJClose_')
        benchmark = '(Lag( (Close_ * DividendFactor * SplitFactor) , 1) ' + \
                    'over (partition by IdcCode order by Date_)) ' + \
                    'as LAGADJClose_'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'LAGADJClose_')

    def test_rolling_avg(self):
        result = sqlf._rolling_avg(10)
        benchmark = '(avg((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between ' + \
                    '9 preceding and current row)) '
        self.assertEqual(clean(result), clean(benchmark))

    def test_rolling_std(self):
        result = sqlf._rolling_std(19)
        benchmark = ' (stdev((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between ' + \
                    '18 preceding and current row)) '
        self.assertEqual(clean(result), clean(benchmark))

    def test_adjust_price(self):
        result = sqlf._adjust_price('ADJClose_')
        benchmark = ' (Close_ * DividendFactor * SplitFactor)  as ADJClose_ '
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'ADJClose_')

    def test_adj(self):
        result = sqlf._adj('Close_')
        benchmark = '(Close_ * DividendFactor * SplitFactor)'
        self.assertEqual(result, benchmark)

    def test_prma(self):
        result = sqlf._prma('PRMA10')
        benchmark = ' (Close_ * DividendFactor * SplitFactor) / ' + \
                    '(avg((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between 9 ' + \
                    'preceding and current row)) as PRMA10'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'PRMA10')
        #
        result = sqlf._prma('PRMA39')
        benchmark = ' (Close_ * DividendFactor * SplitFactor) / ' + \
                    '(avg((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between 38 ' + \
                    'preceding and current row)) as PRMA39'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'PRMA39')

    def test_bollinger(self):
        result = sqlf._bollinger('BOLL20')
        benchmark = \
            '( (Close_ * DividendFactor * SplitFactor) - ' + \
            '( (avg((Close_ * DividendFactor * SplitFactor)) over ' + \
            '(partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) - 2 * (stdev((Close_ * ' + \
            'DividendFactor * SplitFactor)) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ')) / nullif(((avg((Close_ * DividendFactor * SplitFactor))' + \
            'over (partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) + 2 * (stdev((Close_ * ' + \
            'DividendFactor * SplitFactor)) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ') - ( (avg((Close_ * DividendFactor * SplitFactor)) over ' + \
            '(partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) - 2 * (stdev((Close_ * ' + \
            'DividendFactor * SplitFactor)) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)' + \
            ')), 0) as BOLL20'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'BOLL20')

    def test_sqlcmd_from_feature_list(self):
        features = ['PRMA10', 'BOLL20', 'Close_']
        result = sqlf.sqlcmd_from_feature_list(features)
        # No test written
        features = ['PRMA10', 'Close_', 'AS@*%&$@']
        result = sqlf.sqlcmd_from_feature_list(features)
        self.assertListEqual(result[0], ['PRMA10', 'Close_'])

    def tearDown(self):
        pass


def clean(string):
    return string.replace('\n','').replace(' ', '')


if __name__ == '__main__':
    unittest.main()
