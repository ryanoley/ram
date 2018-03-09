import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from StringIO import StringIO
import matplotlib.pyplot as plt

from google.cloud import storage

from ram.utils.time_funcs import convert_date_array

from ram import config
from ram.analysis.statistics import get_stats
from ram.analysis.selection import basic_model_selection


class RunManager(object):

    def __init__(self,
                 strategy_class,
                 run_name,
                 start_year=1950,
                 test_periods=6,
                 drop_params=None,
                 keep_params=None,
                 gcp_cloud_implementation=config.GCP_CLOUD_IMPLEMENTATION,
                 simulation_data_path=config.SIMULATIONS_DATA_DIR):
        """
        Parameters
        ----------
        strategy_class : str
        run_name : str
        start_year : int
        test_periods : int
        drop_params/keep_params : list
            Example: [(param1, 25), (param2, 10)]
        gcp_cloud_implementation : bool
        simulation_data_path : str
        """
        self.strategy_class = strategy_class
        self.run_name = run_name
        self.start_year = start_year
        self.test_periods = test_periods
        self.drop_params = drop_params
        self.keep_params = keep_params
        #
        self._cloud_flag = gcp_cloud_implementation
        if gcp_cloud_implementation:
            self._gcp_client = storage.Client()
            self._gcp_bucket = self._gcp_client.get_bucket(
                config.GCP_STORAGE_BUCKET_NAME)
        else:
            self._simulation_data_path = simulation_data_path

    # ~~~~~~ Viewing Available Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @staticmethod
    def get_strategies():
        """
        Returns all available StrategyClass names in whatever environment
        """
        if config.GCP_CLOUD_IMPLEMENTATION:
            gcp_client = storage.Client()
            bucket = gcp_client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
            cursor = bucket.list_blobs(prefix='simulations/')
            all_files = [x.name for x in cursor]
            all_files.remove('simulations/')
            strategies = list(set([x.split('/')[1] for x in all_files]))
            strategies.sort()
            return strategies
        else:
            path = config.SIMULATIONS_DATA_DIR
            return [x for x in os.listdir(path) if
                    os.path.isdir(os.path.join(path, x))]

    @classmethod
    def get_run_names(cls, strategy_class):
        run_names = cls._get_run_names(strategy_class)
        output = pd.DataFrame()
        for i, (run_name, path) in enumerate(zip(*run_names)):
            if config.GCP_CLOUD_IMPLEMENTATION:
                gcp_client = storage.Client()
                bucket = gcp_client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
                meta = read_json_cloud(path, bucket)
            else:
                meta = read_json(path)
            output.loc[i, 'RunName'] = run_name
            output.loc[i, 'RunDate'] = meta['start_time'][:10] if \
                'start_time' in meta else None
            output.loc[i, 'Completed'] = meta['completed'] if \
                'completed' in meta else None
            output.loc[i, 'Description'] = meta['description']
        return output

    @staticmethod
    def _get_run_names(strategy_class):
        """
        Gets a list of all run names, and the path to the meta file
        """
        if config.GCP_CLOUD_IMPLEMENTATION:
            gcp_client = storage.Client()
            bucket = gcp_client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
            cursor = bucket.list_blobs(
                prefix='simulations/{}/'.format(strategy_class))
            all_files = [x.name for x in cursor]
            run_names = list(set([x.split('/')[2] for x in all_files]))
            run_names.sort()
            # Make path names
            paths = [
                'simulations/{}/{}/meta.json'.format(strategy_class, run) for
                run in run_names]
            return run_names, paths
        else:
            ddir = os.path.join(config.SIMULATIONS_DATA_DIR,
                                strategy_class)
            run_names = [x for x in os.listdir(ddir) if x.find('run') >= 0]
            # Make path names
            paths = ['{}/{}/meta.json'.format(ddir, run) for run in run_names]
            return run_names, paths

    # ~~~~~~ Import Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def import_return_frame(self):
        file_paths = self._get_run_file_paths(filter_text='returns.csv')
        # Trim files for test periods
        if self.test_periods > 0:
            file_paths = file_paths[:-self.test_periods]
        returns = pd.DataFrame()
        for path in file_paths:
            file_year = int(path.split('/')[-1][:4])
            if file_year < self.start_year:
                continue
            if self._cloud_flag:
                temp = read_csv_cloud(path, self._gcp_bucket)
            else:
                temp = pd.read_csv(path, index_col=0)
            returns = returns.add(temp, fill_value=0)
        returns.index = convert_date_array(returns.index)
        # Format columns as strings
        returns.columns = returns.columns.astype(str)
        returns = self._check_import_drop_params(returns)
        self.returns = returns

    def import_stats(self):
        file_paths = self._get_run_file_paths('stats.json')
        self.stats = {}
        if not file_paths:
            return
        for path in file_paths:
            if self._cloud_flag:
                stats = read_json_cloud(path, self._gcp_bucket)
            else:
                stats = read_json(path)
            name = path.split('/')[-1]
            self.stats[name] = stats

    def import_column_params(self):
        file_path = self._get_run_file_paths('column_params.json')[0]
        if self._cloud_flag:
            params = read_json_cloud(file_path, self._gcp_bucket)
        else:
            params = read_json(file_path)
        # Convert keys to strings
        column_params = {}
        for k, v in params.iteritems():
            column_params[str(k)] = v
        self.column_params = column_params

    def import_meta(self):
        file_path = self._get_run_file_paths('meta.json')[0]
        if self._cloud_flag:
            self.meta = read_json_cloud(file_path, self._gcp_bucket)
        else:
            self.meta = read_json(file_path)

    def import_all_output(self):
        file_paths = self._get_run_file_paths('all_output.csv')
        # Trim files for test periods
        if self.test_periods > 0:
            file_paths = file_paths[:-self.test_periods]
        output = pd.DataFrame()
        for path in file_paths:
            file_year = path.split('/')[-1]
            if int(file_year[:4]) < self.start_year:
                continue
            if self._cloud_flag:
                temp = read_csv_cloud(path, self._gcp_bucket)
            else:
                temp = pd.read_csv(path, index_col=0)
            output = output.add(temp, fill_value=0)
        output.index = convert_date_array(output.index)
        self.all_output = output

    def _get_run_file_paths(self, filter_text):
        if self._cloud_flag:
            prefix = 'simulations/{}/{}/'.format(self.strategy_class,
                                                 self.run_name)
            cursor = self._gcp_bucket.list_blobs(prefix=prefix)
            all_blobs = [x.name for x in cursor]
            filtered_blobs = [x for x in all_blobs if x.find(filter_text) >= 0]
            filtered_blobs.sort()
            return filtered_blobs

        else:
            path = self._simulation_data_path
            ddir = os.path.join(path, self.strategy_class, self.run_name)
            all_files = []
            for dir_vals in os.walk(ddir):
                root = dir_vals[0].replace(ddir+'/', '')
                dir_files = [os.path.join(root, x) for x in dir_vals[2]]
                all_files += dir_files
            filtered_files = [x for x in all_files if x.find(filter_text) >= 0]
            filtered_files.sort()
            paths = [os.path.join(ddir, x) for x in filtered_files]
            return paths

    def _check_import_drop_params(self, returns):
        # Do we need to drop parameters as per init?
        if self.drop_params:
            if not hasattr(self, 'column_params'):
                self.import_column_params()
            cparams = classify_params(self.column_params)
            cparams = filter_classified_params(cparams, self.drop_params)
            # Need to drop and reclassify
            # Get unique column names
            cols = get_columns(cparams)
            returns = returns[cols]
            self.column_params = post_drop_params_filter(
                cols, self.column_params)
        if self.keep_params:
            if not hasattr(self, 'column_params'):
                self.import_column_params()
            cparams = classify_params(self.column_params)
            cparams = filter_classified_params_keep(cparams, self.keep_params)
            # Get unique column names
            cols = get_columns(cparams)
            returns = returns[cols]
            self.column_params = post_drop_params_filter(
                cols, self.column_params)
        return returns

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

    def analyze_returns(self, drop_params=None):
        if not hasattr(self, 'returns'):
            self.import_return_frame()
        if drop_params and (not hasattr(self, 'column_params')):
            self.import_column_params()

        if drop_params:
            cparams = classify_params(self.column_params)
            cparams = filter_classified_params(cparams, drop_params)
            # Get unique column names
            cols = get_columns(cparams)
            temp_returns = self.returns[cols]
        else:
            temp_returns = self.returns

        rets1 = basic_model_selection(temp_returns, window=100).iloc[101:]
        rets2 = basic_model_selection(temp_returns, window=100,
                                      criteria='sharpe').iloc[101:]
        rets = pd.DataFrame(rets1)
        rets.columns = ['ReturnOptim']
        rets['SharpeOptim'] = rets2
        return get_stats(rets), get_quarterly_rets(rets,
                                                   column_name='SharpeOptim')

    def plot_results(self, drop_params=None):
        if not hasattr(self, 'returns'):
            self.import_return_frame()
        if drop_params and (not hasattr(self, 'column_params')):
            self.import_column_params()
        if drop_params:
            cparams = classify_params(self.column_params)
            cparams = filter_classified_params(cparams, drop_params)
            # Get unique column names
            cols = get_columns(cparams)
            temp_returns = self.returns[cols]
        else:
            temp_returns = self.returns
        rets1 = basic_model_selection(temp_returns, window=100).iloc[101:]
        rets2 = basic_model_selection(temp_returns, window=100,
                                      criteria='sharpe').iloc[101:]
        temp_returns = temp_returns.loc[rets1.index]
        plt.figure()
        plt.plot(temp_returns.cumsum(), 'b', alpha=0.3)
        plt.plot(rets1.cumsum(), 'r')
        plt.plot(rets2.cumsum(), 'g')
        plt.show()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


def filter_classified_params_keep(cparams, keep_params=None):
    if keep_params:
        assert isinstance(keep_params, list)
        assert isinstance(keep_params[0], tuple)
        # Collect all columns to drop, and pop the parameter
        # should be dropped so it isn't reported
        keep_columns = []
        for dp in keep_params:
            if dp[0] in cparams:
                if str(dp[1]) in cparams[dp[0]]:
                    keep_columns.append(cparams[dp[0]][str(dp[1])])

        # Get intersection of all the keep params
        keep_columns2 = set(keep_columns[0])
        for kc in keep_columns[1:]:
            keep_columns2 = keep_columns2.intersection(set(kc))

        output = {}
        for param, pmap in cparams.items():
            output[param] = {}
            for val, col_list in pmap.items():
                updated_cols = set(col_list).intersection(keep_columns2)
                if updated_cols:
                    output[param][val] = list(updated_cols)
        return output
    return cparams


def get_quarterly_rets(data, column_name):
    data = data.copy()
    data['year'] = [d.year for d in data.index]
    data['qtr'] = [(d.month-1)/3 + 1 for d in data.index]
    data2 = data.groupby(['year', 'qtr'])[column_name].sum().reset_index()
    return data2.pivot(index='year', columns='qtr', values=column_name)


def get_columns(param_dict):
    cols = []
    for vals1 in param_dict.values():
        for vals2 in vals1.values():
            cols.append(vals2)
    cols = list(set(sum(cols, [])))
    cols = np.array(cols)[np.argsort(np.array(cols, dtype=np.int))]
    return cols.tolist()


def post_drop_params_filter(keep_cols, column_params):
    new_column_params = {}
    for c in keep_cols:
        new_column_params[c] = column_params[c]
    return new_column_params


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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _format_columns(columns):
    if not isinstance(columns, list):
        columns = [columns]
    if not isinstance(columns[0], str):
        columns = [str(x) for x in columns]
    return columns


def _get_date_indexes(date_index, start_year):
    years = np.array([x.year for x in date_index])
    return date_index[years >= start_year]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def read_json(path):
    return json.load(open(path, 'r'))


def read_json_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return json.loads(blob.download_as_string())


def read_csv_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return pd.read_csv(StringIO(blob.download_as_string()), index_col=0)
