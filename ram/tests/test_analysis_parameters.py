import unittest
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal

from ram.analysis.parameters import *


class TestAnalysisParameters(unittest.TestCase):

    def setUp(self):
        pass

    def test_classify_params(self):
        result = classify_params({
            '0': {'V1': 1, 'V2': 44, 'V3': 888},
            '1': {'V1': 1, 'V2': 55, 'V3': 999},
            '2': {'V1': 2, 'V2': 44, 'V3': 1111},
            '3': {'V1': 2, 'V2': 44, 'V3': 2222},
            '4': {'V1': 3, 'V2': 55, 'V3': 2222},
            '5': {'V1': 3, 'V2': 55, 'V3': 999},
        })
        benchmark = {
            'V1': {1: ['0', '1'],
                   2: ['2', '3'],
                   3: ['4', '5']},
            'V2': {44: ['0', '2', '3'],
                   55: ['1', '4', '5']},
            'V3': {888: ['0'],
                   999: ['1', '5'],
                   1111: ['2'],
                   2222: ['3', '4']}
        }
        self.assertDictEqual(result, benchmark)

    def test_aggregate_statistics(self):
        stats = {
            'time1': {
                'col1': {
                    'stat1': 0.01,
                    'stat2': 0.02
                },
                'col2': {
                    'stat1': 0.03,
                    'stat2': 0.04
                }
            },
            'time2': {
                'col1': {
                    'stat1': 0.03,
                    'stat2': 0.04
                },
                'col2': {
                    'stat1': 0.05,
                    'stat2': 0.06
                }
            }
        }
        result = aggregate_statistics(stats)
        # Round results due to float imprecision
        for key in result.keys():
            for stat in result[key].keys():
                x = result[key][stat]
                result[key][stat] = (round(x[0], 2), round(x[1], 2))
        benchmark = {
            'col1':
                {
                    'stat1': (0.02, 0.01),
                    'stat2': (0.03, 0.01)
                },
            'col2':
                {
                    'stat1': (0.04, 0.01),
                    'stat2': (0.05, 0.01)
                },
            }
        self.assertDictEqual(result, benchmark)

    def test_format_param_results(self):
        dates = [dt.datetime(2015, 1, 1),
                 dt.datetime(2015, 1, 2),
                 dt.datetime(2015, 1, 3),
                 dt.datetime(2015, 1, 4)]
        data = pd.DataFrame({
            '0': [1, 2, 3, 4],
            '1': [3, 2, 4, 3],
            '2': [99, 2, 3, 4]
        }, index=dates)
        data.index.name = 'Date'
        cparams = {
            'V1': {1: ['0', '2'],
                   2: ['1']},
            'V2': {1: ['0', '1'],
                   2: ['2']}
        }
        astats = {
            '0':
                {
                    'stat1': (0.01, 0.01),
                    'stat2': (0.02, 0.01)
                },
            '1':
                {
                    'stat1': (0.11, 0.01),
                    'stat2': (0.12, 0.01)
                },
            '2':
                {
                    'stat1': (0.21, 0.01),
                    'stat2': (0.22, 0.01)
                },
            }
        result = format_param_results(data, cparams, astats)
        benchmark = pd.DataFrame(index=range(4))
        benchmark['Param'] = ['V1', 'V1', 'V2', 'V2']
        benchmark['Val'] = [1, 2, 1, 2]
        benchmark['Count'] = [2, 1, 2, 1]
        benchmark['MeanTotalRet'] = [59, 12, 11, 108.]
        benchmark['MeanSharpe'] = [1.249455, 3.674235, 2.805363, 0.562419]
        benchmark['stat1'] = [.11, .11, .06, .21]
        benchmark['stat2'] = [.12, .12, .07, .22]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
