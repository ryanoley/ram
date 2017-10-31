import os
import pandas as pd

from sklearn.ensemble import ExtraTreesClassifier
from ram.analysis.combo_search import CombinationSearch
from ram.analysis.run_manager import RunManager



path = os.path.join(os.getenv('DATA'), 'ram', 'implementation',
                    'LongPeadStrategy', 'runs_param_selection')

drop_params = [('drop_ibes', True)]

# SECTOR 20
if True:
    import pdb; pdb.set_trace()
    run01 = RunManager('LongPeadStrategy', 'run_0106',
                        test_periods=0,
                        drop_params=drop_params,
                        simulation_data_path=path)





    run02 = RunManager('LongPeadStrategy', 'run_0109',
                        test_periods=0,
                        simulation_data_path=path)
    
    run03 = RunManager('LongPeadStrategy', 'run_0110',
                        test_periods=0,
                        simulation_data_path=path)
    
    # SECTOR 25
    run04 = RunManager('LongPeadStrategy', 'run_0107',
                        test_periods=0,
                        drop_params=drop_params,
                        simulation_data_path=path)
    
    run05 = RunManager('LongPeadStrategy', 'run_0112',
                        test_periods=0,
                        simulation_data_path=path)
    
    run06 = RunManager('LongPeadStrategy', 'run_0114',
                        test_periods=0,
                        simulation_data_path=path)
    
    # SECTOR 45
    run07 = RunManager('LongPeadStrategy', 'run_0108',
                        test_periods=0,
                        drop_params=drop_params,
                        simulation_data_path=path)
    
    run08 = RunManager('LongPeadStrategy', 'run_0113',
                        test_periods=0,
                        simulation_data_path=path)
    
    run09 = RunManager('LongPeadStrategy', 'run_0115',
                        test_periods=0,
                        simulation_data_path=path)
    
    
    comb = CombinationSearch()
    
    
    comb.add_run(run01)
    comb.add_run(run02)
    comb.add_run(run03)
    comb.add_run(run04)
    comb.add_run(run05)
    comb.add_run(run06)
    comb.add_run(run07)
    comb.add_run(run08)
    comb.add_run(run09)
    
    comb.start(1)
    