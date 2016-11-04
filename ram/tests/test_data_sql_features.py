import unittest
import numpy as np

import ram.data.sql_features as sqlf


class TestSqlFeatures(unittest.TestCase):

    def setUp(self):
        pass

    def test_actual_price(self):
        result = sqlf._actual_price('P_Open')
        benchmark = 'Open_ as P_Open'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'P_Open')

    def test_vol(self):
        result = sqlf._vol('VOL_30_Open')
        benchmark = """
            (Open_ * DividendFactor * SplitFactor) as Open_VOL30,
            (Lag((Open_ * DividendFactor * SplitFactor), 1) over (
                partition by IdcCode
                order by Date_))
            as LagOpen_VOL30
        """
        self.assertEqual(clean(result[0]), clean(benchmark))
        benchmark = """
            stdev(Open_VOL30 / LagOpen_VOL30) over (
            partition by IdcCode
            order by Date_
            rows between 29 preceding and current row) as VOL_30_Open
        """
        self.assertEqual(clean(result[1]), clean(benchmark))

    def test_lag(self):
        result = sqlf._lag('Close_', 2)
        benchmark = '(Lag(Close_, 2) ' + \
                    'over (partition by IdcCode order by Date_))'
        self.assertEqual(clean(result), clean(benchmark))

    def test_lag_adj_price(self):
        result = sqlf._lag_adj_price('LAGADJ_2_Close')
        benchmark = '(Lag( (Close_ * DividendFactor * SplitFactor) , 2) ' + \
                    'over (partition by IdcCode order by Date_)) ' + \
                    'as LAGADJ_2_Close'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'LAGADJ_2_Close')

    def test_rolling_avg(self):
        result = sqlf._rolling_avg('High', 10)
        benchmark = '(avg((High * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between ' + \
                    '9 preceding and current row)) '
        self.assertEqual(clean(result), clean(benchmark))

    def test_rolling_std(self):
        result = sqlf._rolling_std('Close_', 19)
        benchmark = '(stdev((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between ' + \
                    '18 preceding and current row)) '
        self.assertEqual(clean(result), clean(benchmark))

    def test_adjust_price(self):
        result = sqlf._adjust_price('ADJ_Close')
        benchmark = ' (Close_ * DividendFactor * SplitFactor)  as ADJ_Close'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'ADJ_Close')

    def test_adj(self):
        result = sqlf._adj('Close_')
        benchmark = '(Close_ * DividendFactor * SplitFactor)'
        self.assertEqual(result, benchmark)

    def test_prma(self):
        feature = 'PRMA_10_Close'
        result = sqlf._prma(feature)
        benchmark = ' (Close_ * DividendFactor * SplitFactor) / ' + \
                    '(avg((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between 9 ' + \
                    'preceding and current row)) as PRMA_10_Close'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'PRMA_10_Close')
        #
        result = sqlf._prma('PRMA_39_Open')
        benchmark = ' (Open_ * DividendFactor * SplitFactor) / ' + \
                    '(avg((Open_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between 38 ' + \
                    'preceding and current row)) as PRMA_39_Open'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'PRMA_39_Open')

    def test_ma(self):
        feature = 'MA_10_Close'
        result = sqlf._ma(feature)
        benchmark = '(avg((Close_ * DividendFactor * SplitFactor)) over ' + \
                    '(partition by IdcCode order by Date_ rows between 9 ' + \
                    'preceding and current row)) as MA_10_Close'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'MA_10_Close')

    def test_lag_ma(self):
        feature = 'LAGMA_10_Close'
        result = sqlf._lag_ma(feature)
        benchmark = """
            (avg((Close_ * DividendFactor * SplitFactor)) over
            (partition by IdcCode
            order by Date_
            rows between 9 preceding and current row))
            as LAGMA_10_Closetemp
        """
        self.assertEqual(clean(result[0]), clean(benchmark))
        benchmark = """
            (Lag(LAGMA_10_Closetemp, 1) over (
            partition by IdcCode
            order by Date_))
            as LAGMA_10_Close
        """
        self.assertEqual(clean(result[1]), clean(benchmark))

    def test_bollinger(self):
        result = sqlf._bollinger('BOLL_20_Close')
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
            ')), 0) as BOLL_20_Close'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'BOLL_20_Close')
        # High
        result = sqlf._bollinger('BOLL_20_High')
        benchmark = \
            '( (High * DividendFactor * SplitFactor) - ' + \
            '( (avg((High * DividendFactor * SplitFactor)) over ' + \
            '(partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) - 2 * (stdev((High * ' + \
            'DividendFactor * SplitFactor)) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ')) / nullif(((avg((High * DividendFactor * SplitFactor))' + \
            'over (partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) + 2 * (stdev((High * ' + \
            'DividendFactor * SplitFactor)) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)) ' + \
            ') - ( (avg((High * DividendFactor * SplitFactor)) over ' + \
            '(partition by IdcCode order by Date_ rows between 19 ' + \
            'preceding and current row)) - 2 * (stdev((High * ' + \
            'DividendFactor * SplitFactor)) over (partition by IdcCode ' + \
            'order by Date_ rows between 19 preceding and current row)' + \
            ')), 0) as BOLL_20_High'
        self.assertEqual(clean(result[0]), clean(benchmark))
        self.assertEqual(clean(result[1]), 'BOLL_20_High')

    def test_sqlcmd_from_feature_list(self):
        features = ['PRMA_10_Close', 'BOLL_20_Open', 'P_Close']
        result = sqlf.sqlcmd_from_feature_list(features)
        self.assertListEqual(result[0], features)
        # No test written
        features = ['PRMA_10_Close', 'P_Close', 'AS@*%&$@']
        result = sqlf.sqlcmd_from_feature_list(features)
        self.assertListEqual(result[0], ['PRMA_10_Close', 'P_Close'])

    def tearDown(self):
        pass


def clean(string):
    return string.replace('\n', '').replace(' ', '')


if __name__ == '__main__':
    unittest.main()
