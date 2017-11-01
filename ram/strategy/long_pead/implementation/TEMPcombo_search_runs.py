import os
import pandas as pd

from sklearn.ensemble import ExtraTreesClassifier
from ram.analysis.combo_search import CombinationSearch
from ram.analysis.run_manager import RunManager
from ram.analysis.run_manager import RunManagerGCP

drop_params = [('drop_ibes', True)]

runs = [
    'run_0106', 'run_0109', 'run_0110',
    'run_0107', 'run_0112', 'run_0114',
    'run_0108', 'run_0113', 'run_0115',
]

runs = runs[:2]

comb = CombinationSearch(write_flag=True, gcp_implementation=True)

for run in runs:
    run01 = RunManager('LongPeadStrategy', run,
                        test_periods=0,
                        drop_params=drop_params)
    comb.add_run(run01)


if True:
    import pdb; pdb.set_trace()
    comb.start(1)


