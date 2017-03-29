import pandas as pd

from ram.analysis.run_manager import RunManager


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
        return agg_returns
