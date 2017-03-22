import os
import shutil
import unittest
import datetime as dt

from ram.data.constructor import DataConstructor


class TestDataConstructor(unittest.TestCase):

    def setUp(self):
        self.ddir = os.path.join(os.getenv('GITHUB'), 'ram', 'ram', 'tests')

    def test_make_output_directory(self):
        dc = DataConstructor('TestStrategy')
        # Manually override write directory
        dc._prepped_data_dir = os.path.join(self.ddir, 'TestStrategy')
        if os.path.isdir(os.path.join(self.ddir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))
        dc._make_output_directory()
        dc._make_output_directory()
        results = os.listdir(os.path.join(self.ddir, 'TestStrategy'))
        self.assertListEqual(results, ['version_001', 'version_002'])
        shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))

    def test_make_date_iterator(self):
        dc = DataConstructor('TestStrategy')
        dc.register_dates_parameters('Q', 4, 2017)
        dc._make_date_iterator()
        result = dc._date_iterator[0]
        self.assertEqual(result[0], dt.datetime(2016, 1, 1))
        self.assertEqual(result[1], dt.datetime(2017, 1, 1))
        self.assertEqual(result[2], dt.datetime(2017, 4, 1))

    def test_register_universe_size(self):
        dc = DataConstructor('TestStrategy')
        self.assertEqual(dc.filter_args['univ_size'], 500)
        dc.register_universe_size(10)
        self.assertEqual(dc.filter_args['univ_size'], 10)

    def test_run(self):
        dc = DataConstructor('TestStrategy')
        dc._prepped_data_dir = os.path.join(self.ddir, 'TestStrategy')
        if os.path.isdir(os.path.join(self.ddir, 'TestStrategy')):
            shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))
        dc.register_dates_parameters('Q', 4, 2017)
        dc.register_features(['AvgDolVol', 'PRMA10_Close'])
        dc.register_universe_size(10)
        dc.run()
        result = os.listdir(os.path.join(self.ddir, 'TestStrategy',
                                         'version_001'))
        self.assertEquals(result[0], '20170101_data.csv')
        shutil.rmtree(os.path.join(self.ddir, 'TestStrategy'))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
