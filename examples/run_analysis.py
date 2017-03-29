
import matplotlib.pyplot as plt

from ram.analysis.run_manager import RunManager
from ram.analysis.run_aggregator import RunAggregator
from ram.analysis.parameters import analyze_parameters
from ram.analysis.combo_search import CombinationSearch


# View all available strategies
print RunManager.get_strategies()
print RunManager.get_run_names('StatArbStrategy')

rm1 = RunManager('BirdsStrategy', 'run_0005')
rm2 = RunManager('StatArbStrategy', 'run_0021')

#results = rm2.analyze_parameters()

#plt.figure()
#plt.plot(rm2.returns.cumsum())
#plt.show()



# ~~~~~~ RunAggregator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ra = RunAggregator()

ra.add_run(rm1)
ra.add_run(rm2)

if True:
    import pdb; pdb.set_trace()
    returns = ra.aggregate_returns()



# Then after a while hit control C

comb = CombinationSearch()
comb.attach_data(returns)
comb.start()









