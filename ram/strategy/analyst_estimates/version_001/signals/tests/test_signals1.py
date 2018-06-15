import unittest
import numpy as np
import pandas as pd
import datetime as dt

from sklearn.ensemble import RandomForestRegressor
from ram.strategy.analyst_estimates.version_001.signals.signals1 import SignalModel1
from ram.strategy.analyst_estimates.version_001.data.data_container1 import DataContainer1

from pandas.util.testing import assert_frame_equal


class TestSignalModel1(unittest.TestCase):

    def setUp(self):
        train = pd.DataFrame({
                'SecCode': range(1,11),
                'Date': pd.date_range(dt.date(2015,1,1), dt.date(2015,1,10)),
                'T': [1, 2] * 5,
                'Var1': np.random.rand(10),
                'Var2': np.random.rand(10),
                'Response':np.random.rand(10)
        })
        test = pd.DataFrame({
                'SecCode': range(1,11),
                'Date': pd.date_range(dt.date(2015,1,1), dt.date(2015,1,10)),
                'T': [1, 2] * 5,
                'Var1': np.random.rand(10),
                'Var2': np.random.rand(10)
        })

        dc = DataContainer1()
        dc._set_features(inp_features=['Var1', 'Var2'])
        dc.train_data = train
        dc.test_data = test
        dc._entry_window = 3
        self.dc = dc
        self.data = (train, test)

    def test_rf_signals(self):
        sm = SignalModel1()
        sm.rf_signals(self.dc, 1., 1, 10, False, seed=123)
        result = sm.preds_data

        benchmark = {}
        train, test = self.data
        rfr = RandomForestRegressor(n_estimators=10,
                                       min_samples_leaf=1,
                                       max_features=1.)
        rfr.random_state=123

        i_train = train[train['T'] == 1].copy()
        i_test = test[test['T'] == 1].copy()
        rfr.fit(i_train[['Var1','Var2']], i_train['Response'])
        preds = rfr.predict(i_test[['Var1','Var2']])
        output = i_test.copy()
        output['preds'] = preds
        benchmark[2] = output[['SecCode','Date','preds']]
        assert_frame_equal(result[2], benchmark[2])

        i_train = train[train['T'] == 2].copy()
        i_test = test[test['T'] == 2].copy()
        rfr.fit(i_train[['Var1','Var2']], i_train['Response'])
        preds = rfr.predict(i_test[['Var1','Var2']])
        output = i_test.copy()
        output['preds'] = preds
        benchmark[3] = output[['SecCode','Date','preds']]
        assert_frame_equal(result[3], benchmark[3])

        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()


