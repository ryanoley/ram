import unittest
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal

from ram.analysis.parameters import classify_params, format_param_results


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
        result = format_param_results(data, cparams)
        benchmark = pd.DataFrame(index=range(4))
        benchmark['Param'] = ['V1', 'V1', 'V2', 'V2']
        benchmark['Val'] = [1, 2, 1, 2]
        benchmark['Count'] = [2, 1, 2, 1]
        benchmark['MeanTotalRet'] = [59, 12, 11, 108.]
        benchmark['MeanSharpe'] = [1.249455, 3.674235, 2.805363, 0.562419]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
