import os
import importlib

from ram.utils.statistics import create_strategy_report

OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'strategy_output')

# Committed Strategies - Move to config?
#   ('folder', 'strategyclass')

STRATS = [
    ('vxx', 'VXXStrategy'),
    ('gap', 'GapStrategy'),
    ('statarb', 'StatArbStrategy')
]

print 'Updating strategies...'

for strat in STRATS:

    mdir = strat[0]
    mclass = strat[1]
    module = importlib.import_module('ram.strategy.{0}.main'.format(mdir))

    Strategy = getattr(module, mclass)

    strat = Strategy()
    strat.start()
    results = strat.get_results()

    # Write results
    results.to_csv(os.path.join(OUTDIR, '{0}_returns.csv'.format(mclass)))

    # Stats
    create_strategy_report(results, mclass, OUTDIR)

    print mclass
