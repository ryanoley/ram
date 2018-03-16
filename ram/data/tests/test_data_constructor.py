import os
import json
import shutil
import unittest
import pandas as pd
import datetime as dt

from ram import config
from ram.data.data_constructor import *
from ram.data.data_constructor import _get_versions
from ram.data.data_constructor import _get_meta_data
from ram.data.data_constructor import _get_min_max_dates_counts
from ram.data.data_constructor import _get_strategy_version_stats


from ram.data.data_constructor_blueprint import DataConstructorBlueprint


class TestDataConstructor(unittest.TestCase):

    def setUp(self):
        self.prepped_data_dir = os.path.join(os.getenv('GITHUB'), 'ram',
                                             'ram', 'data',
                                             'test_prepped_data')
        self.implementation_data_dir = os.path.join(
            os.getenv('GITHUB'), 'ram', 'ram', 'data',
            'implementation')
        # Make data directories
        if os.path.isdir(self.prepped_data_dir):
            shutil.rmtree(self.prepped_data_dir)
        if os.path.isdir(self.implementation_data_dir):
            shutil.rmtree(self.implementation_data_dir)
        os.mkdir(self.implementation_data_dir)

    def test_make_output_directory(self):
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        dc = DataConstructor(self.prepped_data_dir)
        dc._make_output_directory(blueprint)
        dc._make_output_directory(blueprint)
        result = os.listdir(os.path.join(self.prepped_data_dir,
                                         'GeneralOutput'))
        benchmark = ['archive', 'version_0001', 'version_0002']
        self.assertListEqual(result, benchmark)

    def test_write_archive_meta_data(self):
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        dc = DataConstructor(self.prepped_data_dir)
        dc._make_output_directory(blueprint)
        dc._write_archive_meta_data(blueprint)
        path = os.path.join(self.prepped_data_dir, 'GeneralOutput',
                            'archive', 'version_0001.json')
        self.assertTrue(os.path.isfile(path))
        path = os.path.join(self.prepped_data_dir, 'GeneralOutput',
                            'version_0001', 'meta.json')
        self.assertTrue(os.path.isfile(path))

    def test_write_archive_meta_data(self):
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        dc = DataConstructor(self.prepped_data_dir)
        self.assertTrue(dc._check_parameters(blueprint))
        del blueprint.universe_filter_arguments['filter']
        with self.assertRaises(AssertionError):
            dc._check_parameters(blueprint),

    def test_make_date_iterator(self):
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        blueprint.universe_date_parameters['train_period_length'] = 1
        blueprint.universe_date_parameters['start_year'] = 2017
        blueprint.universe_date_parameters[
            'quarter_frequency_month_offset'] = 1
        dc = DataConstructor(self.prepped_data_dir)
        result = dc._make_date_iterator(blueprint)
        result = result[0]
        self.assertEqual(result[0], dt.datetime(2016, 11, 1))
        self.assertEqual(result[1], dt.datetime(2017, 2, 1))
        self.assertEqual(result[2], dt.datetime(2017, 4, 30))

    def test_init_rerun(self):
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        dc = DataConstructor(self.prepped_data_dir)
        dc._make_output_directory(blueprint)
        dc._write_archive_meta_data(blueprint)
        # Write some dummy data files
        df = pd.DataFrame({'V1': range(4)})
        df.to_csv(os.path.join(self.prepped_data_dir, 'GeneralOutput',
                               'version_0001', '20101010_data.csv'))
        df.to_csv(os.path.join(self.prepped_data_dir, 'GeneralOutput',
                               'version_0001', '20111010_data.csv'))
        dc._init_rerun('GeneralOutput', 'version_0001')
        result = dc._version_files
        benchmark = ['20101010_data.csv', '20111010_data.csv']
        self.assertListEqual(result, benchmark)

    def test_run_rerun_and_completeness(self):
        # Don't execute on cloud instance
        if config.GCP_CLOUD_IMPLEMENTATION:
            return
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        blueprint.universe_date_parameters['train_period_length'] = 1
        blueprint.universe_date_parameters[
            'quarter_frequency_month_offset'] = 1
        dc = DataConstructor(self.prepped_data_dir)
        dc.run(blueprint)
        # Get files
        result = os.listdir(os.path.join(
            self.prepped_data_dir, 'GeneralOutput', 'version_0001'))
        result.sort()
        self.assertEqual(result[0], '20170201_data.csv')
        # Drop most recent
        result = [x for x in result if x.find('_data.csv') > -1]
        path = os.path.join(self.prepped_data_dir, 'GeneralOutput',
                            'version_0001', result[-1])
        os.remove(path)
        # Shave a few days to recreate two files
        path = os.path.join(self.prepped_data_dir, 'GeneralOutput',
                            'version_0001', result[-2])
        data = pd.read_csv(path)
        data = data[data.Date <= data.Date.unique()[30]]
        data.to_csv(path, index=None)
        dc = DataConstructor(self.prepped_data_dir)
        dc.rerun('GeneralOutput', 'version_0001')
        path = os.path.join(self.prepped_data_dir, 'GeneralOutput',
                            'version_0001', 'meta.json')
        meta = json.load(open(path, 'r'))
        result = meta['newly_created_files']
        benchmark = result[-2:]
        self.assertListEqual(result, benchmark)

    def test_run_market_data(self):
        # Don't execute on cloud instance
        if config.GCP_CLOUD_IMPLEMENTATION:
            return
        blueprint = DataConstructorBlueprint('universe', 'Test description',
                                             market_data_flag=True)
        blueprint.market_data_params['features'] = ['AdjClose']
        blueprint.market_data_params['seccodes'] = [50311, 10955]
        blueprint.universe_date_parameters['train_period_length'] = 1
        dc = DataConstructor(self.prepped_data_dir)
        dc.run(blueprint)
        result = os.listdir(os.path.join(self.prepped_data_dir,
                                         'GeneralOutput', 'version_0001'))
        self.assertTrue('market_index_data.csv' in result)

    def test_random_functions(self):
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        dc = DataConstructor(self.prepped_data_dir)
        dc._make_output_directory(blueprint)
        dc._write_archive_meta_data(blueprint)
        dc._make_output_directory(blueprint)
        dc._write_archive_meta_data(blueprint)
        result = _get_versions(self.prepped_data_dir, 'GeneralOutput')
        benchmark = {0: 'version_0001', 1: 'version_0002'}
        self.assertDictEqual(result, benchmark)
        result = get_data_version_name('GeneralOutput', 0,
                                       prepped_data_dir=self.prepped_data_dir)
        self.assertEqual(result, 'version_0001')
        result = get_data_version_name('GeneralOutput', '0',
                                       prepped_data_dir=self.prepped_data_dir)
        self.assertEqual(result, 'version_0001')
        result = get_data_version_name('GeneralOutput', 'version_0001',
                                       prepped_data_dir=self.prepped_data_dir)
        self.assertEqual(result, 'version_0001')
        with self.assertRaises(Exception) as context:
            get_data_version_name('GeneralOutput', 'version_0010',
                                  prepped_data_dir=self.prepped_data_dir)
        df = pd.DataFrame({'V1': range(4)})
        df.to_csv(os.path.join(self.prepped_data_dir, 'GeneralOutput',
                               'version_0001', '20101010_data.csv'))
        df.to_csv(os.path.join(self.prepped_data_dir, 'GeneralOutput',
                               'version_0001', '20111010_data.csv'))
        result = _get_meta_data(self.prepped_data_dir,
                                'GeneralOutput',
                                'version_0001')
        result = _get_min_max_dates_counts(self.prepped_data_dir,
                                           'GeneralOutput',
                                           'version_0001')
        self.assertEqual(result[0], '20101010')
        self.assertEqual(result[1], '20111010')
        self.assertEqual(result[2], 2)
        result = _get_min_max_dates_counts(self.prepped_data_dir,
                                           'GeneralOutput',
                                           'version_0002')
        self.assertEqual(result[0], 'No Files')
        self.assertEqual(result[1], 'No Files')
        self.assertEqual(result[2], 0)
        result = _get_strategy_version_stats('GeneralOutput',
                                             self.prepped_data_dir)
        print_data_versions('GeneralOutput',
                            prepped_data_dir=self.prepped_data_dir)

    def test_run_live(self):
        blueprint = DataConstructorBlueprint('seccodes', 'Test description')
        blueprint.seccodes_filter_arguments['output_file_name'] = 'asdf'
        dc = DataConstructor(
            ram_implementation_dir=self.implementation_data_dir)
        dc.run_live(blueprint, 'TestStrategy')
        path = os.path.join(self.implementation_data_dir, 'TestStrategy',
                            'daily_raw_data', 'asdf.csv')
        self.assertTrue(os.path.isfile(path))

    def test_make_implementation_dates(self):
        dc = DataConstructor(self.prepped_data_dir)
        blueprint = DataConstructorBlueprint('universe', 'Test description')
        blueprint.constructor_type = 'universe_live'
        blueprint.universe_date_parameters['frequency'] = 'M'
        blueprint.universe_date_parameters['test_period_length'] = 2
        result = dc._make_implementation_dates(blueprint)
        today = dt.date.today()
        benchmark = dt.date(today.year, today.month, 1)
        self.assertEqual(result[1], benchmark)

    def tearDown(self):
        if os.path.isdir(self.prepped_data_dir):
            shutil.rmtree(self.prepped_data_dir)
        if os.path.isdir(self.implementation_data_dir):
            shutil.rmtree(self.implementation_data_dir)

if __name__ == '__main__':
    unittest.main()
