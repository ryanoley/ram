import os
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.long_pead.implementation.get_live_positions import import_models

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.externals import joblib


class TestGetLivePositions(unittest.TestCase):

    def setUp(self):
        # Create some fake models
        X = pd.DataFrame(np.random.randn(1000, 10))
        y = np.random.rand(1000) >= 0.7
        clf = ExtraTreesClassifier()
        clf.fit(X=X, y=y)
        self.test_dir = os.path.join(
            os.getenv('GITHUB'), 'ram', 'ram', 'strategy',
            'long_pead', 'tests')
        self.file_names = ['test1.pkl', 'test2.pkl']
        out_path = os.path.join(self.test_dir, self.file_names[0])
        joblib.dump(clf, out_path)
        out_path = os.path.join(self.test_dir, self.file_names[1])
        joblib.dump(clf, out_path)

    def test_import_models(self):
        result = import_models(self.test_dir, self.file_names)
        self.assertIsInstance(result.values()[0], ExtraTreesClassifier)
        self.assertIsInstance(result.values()[1], ExtraTreesClassifier)
        self.assertIn('test1.pkl', result)
        self.assertIn('test2.pkl', result)

    def tearDown(self):
        for f in self.file_names:
            path = os.path.join(self.test_dir, f)
            if os.path.isfile(path):
                os.remove(path)


if __name__ == '__main__':
    unittest.main()
