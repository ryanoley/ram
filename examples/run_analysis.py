import os
import pandas as pd

from ram import config

import matplotlib.pyplot as plt

from ram.analysis.run_manager import RunManager
from ram.analysis.run_aggregator import RunAggregator

# View all available strategies
print(RunManager.get_strategies())
print(RunManager.get_run_names('StatArbStrategy'))

rm1 = RunManager('StatArbStrategy', 'run_0021', 2009)
rm2 = RunManager('StatArbStrategy', 'run_0022', 2009)
rm3 = RunManager('StatArbStrategy', 'run_0023', 2009)

rm1.import_return_frame()
rm2.import_return_frame()
rm3.import_return_frame()

plt.figure()
plt.plot(rm1.returns.cumsum(), 'b', alpha=.5)
plt.plot(rm2.returns.cumsum(), 'g', alpha=.5)
plt.plot(rm3.returns.cumsum(), 'r', alpha=.5)
plt.show()

rm1.analyze_parameters()
rm2.analyze_parameters()
rm3.analyze_parameters()


# ~~~~~~ RunAggregator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ra = RunAggregator()

ra.add_run(rm1)
ra.add_run(rm2)

returns = ra.aggregate_returns()

plt.figure()
plt.plot(comb.best_results_rets.fillna(0).cumsum())
plt.show()

