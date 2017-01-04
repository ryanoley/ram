import os
import importlib

from ram.utils.statistics import create_strategy_report

OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'strategy_output')

# Committed Strategies - Move to config?
#   ('folder', 'strategyclass')

STRATS = [
    ('vxx', 'VXXStrategy'),
    ('reversion', 'ReversionStrategy'),
    ('benchmarks', 'BenchmarksStrategy'),
    ('gap', 'GapStrategy'),
    ('statarb', 'StatArbStrategy', ('pairs1')),
    ('statarb', 'StatArbStrategy', ('pairs2'))
]

print 'Updating strategies...'

for strat in STRATS:

    mdir = strat[0]
    mclass = strat[1]
    try:
        args = strat[2]
    except:
        args = None

    module = importlib.import_module('ram.strategy.{0}.main'.format(mdir))

    Strategy = getattr(module, mclass)

    if args:
        strat = Strategy(args)
    else:
        strat = Strategy()
    strat.start()
    results = strat.results

    ## TEMP FIX FOR STATARB
    if args:
        mclass += args[0]

    # Write results
    results.to_csv(os.path.join(OUTDIR, '{0}_returns.csv'.format(mclass)))

    # Stats
    create_strategy_report(results, mclass, OUTDIR)

    print mclass
