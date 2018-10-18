import unittest
import numpy as np
import pandas as pd
import datetime as dt

from pandas.util.testing import assert_frame_equal

from gearbox import create_time_index
from ram.analysis.run_manager import RunManager
from ram.strategy.sandbox.base.model_selector import ModelSelector


class TestFeatures(unittest.TestCase):

    def setUp(self):
        run1 = RunManager('TestStrat', 'run_0001')
        dates = [dt.datetime(2015, 1, 1) + dt.timedelta(days=i)
                 for i in range(150)]
        data1 = pd.DataFrame(np.random.randn(150, 20), index=dates)
        run1.returns = data1
        run1.column_params = {str(i): {'V1': i, 'V2': 2} for i in range(20)}
        run1.all_output = pd.DataFrame([])
        self.run1 = run1
        self.ms = ModelSelector(self.run1)


    def test_prep_model_param_data(self):
        result = self.ms.prep_model_param_data(self.run1)
        benchmark = pd.DataFrame(data={'Model':range(20),
                                        'V1':range(20),
                                        'V2':2})
        benchmark.Model = benchmark.Model.astype(str)
        benchmark.sort_values('Model', inplace=True)
        benchmark.reset_index(drop=True, inplace=True)
        assert_frame_equal(result, benchmark)

    def test_get_per_returns(self):
        dates = [dt.date(2015, 1, 1) + dt.timedelta(days=i)
                for i in range(150)]
        model_data = pd.DataFrame(data={'Date':dates,
                                  'Model':np.random.randint(0, 20, 150),
                                  'V1':range(150),
                                  'V2':range(150, 300),
                                  'Resp':np.random.randint(0, 100, 150)})
        model_data['QIndex'] = create_time_index(model_data.Date)

        self.ms._set_features(inp_features=['V1','V2'], inp_mkt_features=[])
        result = self.ms.get_per_returns(model_data, n_models=1,
                                         response_var='Resp', q_start=1)
        pass

    def tearDown(self):
        pass



if __name__ == '__main__':
    unittest.main()

