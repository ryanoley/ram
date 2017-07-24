import os
import json
import numpy as np
import pandas as pd

from ram.utils.time_funcs import convert_date_array

from ram import config
from ram.analysis.statistics import get_stats


class RunManager(object):

    def __init__(self, strategy_class, run_name, start_year=1950,
                 test_periods=6):
        self.strategy_class = strategy_class
        self.run_name = run_name
        self.start_year = start_year
        self.test_periods = test_periods

    # ~~~~~~ Viewing Available Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @staticmethod
    def get_strategies(path=config.SIMULATION_OUTPUT_DIR):
        return [x for x in os.listdir(path) if
                os.path.isdir(os.path.join(path, x))]

    @staticmethod
    def get_run_names(strategy_class, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, strategy_class)
        dirs = [x for x in os.listdir(ddir) if x.find('run') >= 0]
        output = pd.DataFrame({'Run': dirs, 'Description': np.nan})
        for i, d in enumerate(dirs):
            desc = json.load(open(os.path.join(ddir, d, 'meta.json'), 'r'))
            output.loc[i, 'Description'] = desc['description']
            if 'completed' in desc:
                output.loc[i, 'Completed'] = desc['completed']
            else:
                output.loc[i, 'Completed'] = None
        return output[['Run', 'Completed', 'Description']]

    # ~~~~~~ Import Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def import_return_frame(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('returns') > 0]
        # Trim files for test periods
        test_index = max([0, len(files) - self.test_periods])
        test_mult = 1
        returns = pd.DataFrame()
        for i, f in enumerate(files):
            if int(f[:4]) < self.start_year:
                continue
            if i >= test_index:
                test_mult = 0
            returns = returns.add(
                test_mult * pd.read_csv(os.path.join(ddir, f), index_col=0),
                fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.returns = returns

    def import_stats(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('stats.json') > 0]
        self.stats = {}
        if files:
            for f in files:
                self.stats[f] = json.load(open(os.path.join(ddir, f), 'r'))
        else:
            self.stats['20100101NOSTATS'] = {x: {'no_stat': -999} for x
                                             in self.column_params}

    def import_column_params(self, path=config.SIMULATION_OUTPUT_DIR):
        ppath = os.path.join(path, self.strategy_class, self.run_name,
                             'column_params.json')
        self.column_params = json.load(open(ppath, 'r'))

    def import_meta(self, path=config.SIMULATION_OUTPUT_DIR):
        ppath = os.path.join(path, self.strategy_class, self.run_name,
                             'meta.json')
        self.meta = json.load(open(ppath, 'r'))

    # ~~~~~~ Analysis Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def analyze_parameters(self, drop_params=None):
        """
        Parameters
        ----------
        drop_params : list of dicts
        """
        if not hasattr(self, 'returns'):
            self.import_return_frame()
        if not hasattr(self, 'column_params'):
            self.import_column_params()
        if not hasattr(self, 'stats'):
            self.import_stats()
        cparams = classify_params(self.column_params)
        cparams = filter_classified_params(cparams, drop_params)
        astats = aggregate_statistics(self.stats, self.start_year)
        return format_param_results(self.returns, cparams,
                                    astats, self.start_year)

    def analyze_returns(self, columns=None, start_year=1950):
        """
        This is a half-baked implementation. Think about what this could do
        """
        if not hasattr(self, 'returns'):
            self.import_return_frame()
        # Get indexes from imported data
        inds = _get_date_indexes(self.returns.index, start_year)
        if columns:
            return get_stats(self.returns.loc[inds, _format_columns(columns)])
        else:
            return get_stats(self.returns.loc[inds])

    def basic_model_selection(self, window=30, criteria='mean'):
        # Rolling mean, offset by one day and select top
        roll_mean = self.returns.rolling(window=window).mean()
        if criteria == 'sharpe':
            roll_sharpe = roll_mean / self.returns.rolling(window=window).std()
            inds = np.argmax(roll_sharpe.values, axis=1)
        else:
            inds = np.argmax(roll_mean.values, axis=1)
        best_rets = pd.DataFrame(index=self.returns.index)
        best_rets['Rets'] = np.choose(np.roll(inds, 1), self.returns.values.T)
        best_rets.Rets.iloc[:window] = np.nan
        return best_rets


###############################################################################

def filter_classified_params(cparams, drop_params=None):
    if drop_params:
        assert isinstance(drop_params, list)
        assert isinstance(drop_params[0], tuple)
        # Collect all columns to drop, and pop the parameter
        # should be dropped so it isn't reported
        drop_columns = []
        for dp in drop_params:
            if dp[0] in cparams:
                if str(dp[1]) in cparams[dp[0]]:
                    drop_columns.append(cparams[dp[0]].pop(str(dp[1])))
        # Clean out drop_columns from remaining parameters
        drop_columns = set(sum(drop_columns, []))
        output = {}
        for param, pmap in cparams.items():
            output[param] = {}
            for val, col_list in pmap.items():
                if len(list(set(col_list) - drop_columns)):
                    output[param][val] = list(set(col_list) - drop_columns)
        return output
    return cparams


###############################################################################

def _format_columns(columns):
    if not isinstance(columns, list):
        columns = [columns]
    if not isinstance(columns[0], str):
        columns = [str(x) for x in columns]
    return columns


def _get_date_indexes(date_index, start_year):
    years = np.array([x.year for x in date_index])
    return date_index[years >= start_year]


###############################################################################

def aggregate_statistics(stats, start_year):
    """
    When stats are written across multiple files, they must be combined
    into one data point. This function takes in a dictionary that
    has in the first layer the file name, and then the next layer a dictionary
    that has all the stats for each column.

    For example:
    {
        '20100101_stats.json': {
            '0': {'stat1': 10, 'stat2': 20},
            '1': {'stat1': 20, 'stat2': 30}
        },
        '20110101_stats.json': {
            '0': {'stat1': 20, 'stat2': 40},
            '1': {'stat1': 30, 'stat2': 50}
        },
    }

    The routine will take the mean and standard deviation over all the stats
    and return a dictionary that has the column in the keys and a dictionary
    of the stat: means

    For example:
    {
        '0': {'stat1': 15, 'stat2': 30},
        '1': {'stat1': 25, 'stat2': 40}
    }
    """
    agg_stats = {k: {k: [] for k in stats.values()[0].values()[0].keys()}
                 for k in stats.values()[0].keys()}

    for t_index in stats.keys():
        if int(t_index[:4]) < start_year:
            continue
        for col, col_stats in stats[t_index].iteritems():
            for key, val in col_stats.iteritems():
                agg_stats[col][key].append(val)

    # Aggregate column stats overtime
    out_stats = {}
    for col, col_stats in agg_stats.iteritems():
        out_stats[col] = {}
        for key, vals in col_stats.iteritems():
            out_stats[col][key] = (np.mean(vals), np.std(vals))

    return out_stats


def classify_params(params):
    """
    Params is a dictionary that has the column number as a key, and
    a dictionary of the parameters for that particular run as the value.

    For example:
    {
        '0': {'param1': 1, 'param2': 2},
        '1': {'param1': 1, 'param2': 3}
    }

    The output is then a dictionary with parameters in the keys, and
    a list of columns in the values.

    From the above example:
    {
        'param1': {1: [0, 1]},
        'param2': {2: [0], 3: [1]}
    }
    """
    out = {}
    for i, p in params.iteritems():
        for k, v in p.iteritems():
            if str(k) not in out:
                out[str(k)] = {}
            if str(v) not in out[str(k)]:
                out[str(k)][str(v)] = []
            out[str(k)][str(v)].append(i)
            out[str(k)][str(v)].sort()
    return out


def format_param_results(data, cparams, astats, start_year):
    """
    This function aggregates returns and statistics across all the different
    parameters. For example, training_period_length could have two values
    {10, 20}, and there are five of each. This would be represented
    by two lines in the data frame with statistics related to the return
    series ALONG with the statistics that are passed in `astats`.
    """
    out = []
    stat_names = astats.values()[0].keys()
    stat_names.sort()
    for k, p in cparams.iteritems():
        for v, cols in p.iteritems():
            s1 = data.loc[:, cols].sum().mean()
            s2 = (data.loc[:, cols].mean() / data.loc[:, cols].std()).mean()

            col_stats = [astats[c] for c in cols]
            # Agg by stat
            agg_stats = {k: [] for k in stat_names}
            for stat in col_stats:
                for key, val in stat.iteritems():
                    agg_stats[key].append(val[0])
            # Average each stat and add to data
            st_list = []
            for stat in stat_names:
                st_list.append(np.mean(agg_stats[stat]))
            out.append([k, v, len(cols), s1, s2] + st_list)

    out = pd.DataFrame(out, columns=['Param', 'Val', 'Count',
                                     'MeanTotalRet', 'MeanSharpe'] +
                       stat_names)
    out = out.sort_values(['Param', 'Val']).reset_index(drop=True)
    return out
