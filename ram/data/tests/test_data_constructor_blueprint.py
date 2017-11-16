import os
import json
import shutil
import unittest
import pandas as pd
import datetime as dt

from ram.data.data_constructor_blueprint import DataConstructorBlueprint
from ram.data.data_constructor_blueprint import \
    DataConstructorBlueprintContainer


class TestDataConstructorBlueprint(unittest.TestCase):

    def setUp(self):
        pass

    def test_init(self):
        dcb = DataConstructorBlueprint('universe')
        self.assertEqual(dcb.constructor_type, 'universe')
        self.assertEqual(dcb.universe_filter_arguments['filter'], 'AvgDolVol')
        self.assertEqual(dcb.universe_filter_arguments['univ_size'], 10)
        self.assertEqual(dcb.universe_filter_arguments['where'],
                         'MarketCap >= 200 and Close_ between 15 and 1000')
        self.assertEqual(
            dcb.universe_date_parameters['quarter_frequency_month_offset'], 1)
        self.assertEqual(
            dcb.universe_date_parameters['train_period_length'], 4)
        self.assertEqual(dcb.universe_date_parameters['frequency'], 'Q')
        self.assertEqual(dcb.universe_date_parameters['start_year'], 2017)
        self.assertEqual(dcb.universe_date_parameters['test_period_length'], 1)
        self.assertFalse(hasattr(dcb, 'market_data_params'))
        self.assertEqual(dcb.output_dir_name, 'GeneralOutput')
        #
        dcb = DataConstructorBlueprint('universe', market_data_flag=True)
        self.assertTrue(hasattr(dcb, 'market_data_params'))
        #
        dcb = DataConstructorBlueprint('seccodes')
        self.assertEqual(dcb.constructor_type, 'seccodes')
        self.assertEqual(
            dcb.seccodes_filter_arguments['seccodes'], [6027, 36799])
        self.assertEqual(
            dcb.seccodes_filter_arguments['start_date'], '2010-01-01')
        self.assertEqual(
            dcb.seccodes_filter_arguments['end_date'], '2015-01-01')
        #
        dcb = DataConstructorBlueprint('etfs')
        self.assertEqual(dcb.constructor_type, 'etfs')
        self.assertEqual(
            dcb.etfs_filter_arguments['tickers'], ['SPY'])
        self.assertEqual(
            dcb.etfs_filter_arguments['start_date'], '2010-01-01')
        self.assertEqual(
            dcb.etfs_filter_arguments['end_date'], '2015-01-01')

    def test_container(self):
        container = DataConstructorBlueprintContainer()
        dcb = DataConstructorBlueprint('universe')
        container.add_blueprint(dcb, 'container 1')
        container.add_blueprint(dcb, 'container 2')
        result = container._blueprints.keys()
        result.sort()
        benchmark = ['blueprint_0001', 'blueprint_0002']
        self.assertListEqual(result, benchmark)
        bp = container.get_blueprint_by_name_or_index('blueprint_0002')
        bp = container.get_blueprint_by_name_or_index(0)

    def test_to_from_json(self):
        dcb = DataConstructorBlueprint('universe')
        result = dcb.to_json()
        dcb2 = DataConstructorBlueprint(blueprint_json=result)
        self.assertDictEqual(result, dcb2.to_json())

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
