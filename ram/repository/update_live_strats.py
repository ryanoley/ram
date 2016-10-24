
import os
import importlib


# Committed Strategies - Move to config?
#   {'folder': 'strategyclass'}

STRATS = [
    {'vxx': 'VXXStrategy'},
]



for strat in STRATS:
    mdir = strat.keys()[0]
    mclass = strat.values()[0]
    module = importlib.import_module('ram.strategy.{0}.main'.format(mdir))
    Strategy = getattr(module, mclass)

    strat = Strategy()
    strat.start_live()
    strat.get_results()
