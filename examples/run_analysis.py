import os
import pandas as pd

from ram import config

import matplotlib.pyplot as plt

from ram.analysis.run_manager import RunManager
from ram.analysis.run_aggregator import RunAggregator

# View all available strategies
print RunManager.get_strategies()
print RunManager.get_run_names('StatArbStrategy')

rm1 = RunManager('BirdsStrategy', 'run_0005')
rm2 = RunManager('StatArbStrategy', 'run_0021')


rm2.import_return_frame()


path = os.path.join(config.SIMULATION_OUTPUT_DIR, 'StatArbStrategy',
                    'run_0018', 'results.csv')
df = pd.read_csv(path, index_col=0)

from gearbox import convert_date_array
df.index = convert_date_array(df.index)


import datetime as dt

rets1 = rm2.returns
rets2 = df
rets1 = rets1.loc[rets1.index >= rets2.index[0]]


#results = rm2.analyze_parameters()

plt.figure()
plt.plot(rets1.cumsum(), 'b', alpha=.5)
plt.plot(rets2.cumsum(), 'g', alpha=.5)
plt.show()



print rets1.mean().mean()
print rets2.mean().mean()


# ~~~~~~ RunAggregator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ra = RunAggregator()

ra.add_run(rm1)
ra.add_run(rm2)

returns = ra.aggregate_returns()





plt.figure()
plt.plot(comb.best_results_rets.fillna(0).cumsum())
plt.show()









