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
        if hasattr(self, 'returns'):
            print('Returns have already been imported..')
            return
        agg_returns = pd.DataFrame()
        agg_params = {}
        print('Reading and aggregating runs...')
        for run in self.runs:
            # Make sure everything is there
            if not hasattr(run, 'returns'):
                run.import_return_frame()
            if not hasattr(run, 'column_params'):
                run.import_column_params()
            if not hasattr(run, 'meta'):
                run.import_meta()
            # Append name to frame
            prefix = '{}_{}'.format(run.strategy_class, run.run_name)
            tmp = run.returns.copy()
            tmp.columns = ['{}_{}'.format(prefix, x) for x in tmp.columns]
            agg_returns = agg_returns.join(tmp, how='outer')
            for k, v in run.column_params.iteritems():
                # TODO: Is this still necessary?
                try:
                    code_version = run.meta['strategy_code_version']
                except:
                    code_version = 'Not in meta file'
                #
                packet = {
                    'column_params': v,
                    'strategy_class': run.strategy_class,
                    'run_name': run.run_name,
                    'prepped_data_version': run.meta['prepped_data_version'],
                    'strategy_code_version': code_version,
                    'description': run.meta['description'],
                    'blueprint': run.meta['blueprint']
                 }
                agg_params['{}_{}'.format(prefix, k)] = packet
        print('Finished aggregating runs...')
        if agg_returns.index.value_counts().max() > 1:
            raise 'Merged return series have duplicated dates'
        self.returns = agg_returns
        self.column_params = agg_params

    def basic_model_selection(self, window=30, criteria='mean'):
        if not hasattr(self, 'returns'):
            self.aggregate_returns()
        return basic_model_selection(self.returns, window, criteria)
