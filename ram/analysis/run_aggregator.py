import pandas as pd

from ram.analysis.run_manager import RunManager
from ram.analysis.selection import basic_model_selection


class RunAggregator(object):

    def __init__(self):
        self.runs = []

    def add_run(self, run):
        assert isinstance(run, RunManager)
        self.runs.append(run)

    def aggregate_returns(self):
        agg_returns = pd.DataFrame()
        for run in self.runs:
            # Make sure everything is there
            if not hasattr(run, 'returns'):
                run.import_return_frame()
            # Append name to frame
            prefix = '{}_{}'.format(run.strategy_class, run.run_name)
            tmp = run.returns.copy()
            tmp.columns = ['{}_{}'.format(prefix, x) for x in tmp.columns]
            agg_returns = agg_returns.join(tmp, how='outer')
        if agg_returns.index.value_counts().max() > 1:
            raise 'Merged return series have duplicated dates'
        self.returns = agg_returns

    def basic_model_selection(self, window=30, criteria='mean'):
        if not hasattr(self, 'returns'):
            self.aggregate_returns()
        return basic_model_selection(self.returns, window, criteria)
