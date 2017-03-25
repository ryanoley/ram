import os

from ram.strategy_repo.basics.spy_strategy import SpyBasics

strategies = [SpyBasics]

strat = strategies[0]
results = strat().get_daily_returns()
meta = strat().get_meta_data()
