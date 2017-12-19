import unittest

from ram.strategy.analyst_estimates.main import AnalystEstimates, make_arg_iter


class TestMain(unittest.TestCase):

    def setUp(self):
        pass

    def test_make_arg_iter(self):
        parameters = {'V1': [1, 2], 'V2': [3, 4]}
        result = make_arg_iter(parameters)
        benchmark = [{'V1': 1, 'V2': 3}, {'V1': 1, 'V2': 4},
                     {'V1': 2, 'V2': 3}, {'V1': 2, 'V2': 4}]
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
