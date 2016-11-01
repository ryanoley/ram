
import os
import importlib


# Committed Strategies - Move to config?
#   {'folder': 'strategyclass'}

STRATS = [
    ('vxx', 'VXXStrategy'),
    ('benchmark', 'BenchmarksStrategy'),
    ('gap', 'GapStrategy'),
    ('statarb', 'StatArbStrategy')
]


for strat in STRATS:
    mdir = strat[0]
    mclass = strat[1]
    module = importlib.import_module('ram.strategy.{0}.main'.format(mdir))
    Strategy = getattr(module, mclass)

    strat = Strategy()
    strat.start_live()
    results = strat.get_results()
