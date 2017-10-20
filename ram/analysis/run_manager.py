import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from StringIO import StringIO
import matplotlib.pyplot as plt
import seaborn as sns

from google.cloud import storage

from ram.utils.time_funcs import convert_date_array

from ram import config
from ram.analysis.statistics import get_stats
from ram.analysis.selection import basic_model_selection


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
        output = pd.DataFrame({'Run': dirs, 'Description': np.nan,
                               'Starred': ''})
        for i, d in enumerate(dirs):
            desc = json.load(open(os.path.join(ddir, d, 'meta.json'), 'r'))
            output.loc[i, 'Description'] = desc['description']
            if 'completed' in desc:
                output.loc[i, 'Completed'] = desc['completed']
            else:
                output.loc[i, 'Completed'] = None
            if 'end_time' in desc:
                output.loc[i, 'RunDate'] = desc['end_time'][:10]
            elif 'start_time' in desc:
                output.loc[i, 'RunDate'] = desc['start_time'][:10]
            else:
                output.loc[i, 'RunDate'] = None
            if os.path.isfile(os.path.join(ddir, d, 'starred.json')):
                output.loc[i, 'Starred'] = '*'
        return output[['Run', 'RunDate', 'Completed',
                       'Description', 'Starred']]

    # ~~~~~~ Import Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def import_return_frame(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('returns') > 0]
        # Trim files for test periods
        if self.test_periods > 0:
            files = files[:-self.test_periods]
        returns = pd.DataFrame()
        for i, f in enumerate(files):
            if int(f[:4]) < self.start_year:
                continue
            returns = returns.add(
                pd.read_csv(os.path.join(ddir, f), index_col=0),
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

    # TEMP??
    def import_long_short_returns(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('all_output') > 0]
        if len(files) == 0:
            print('No `all_output` files available to analyze')
            self.returns = None
        # Trim files for test periods
        if self.test_periods > 0:
            files = files[:-self.test_periods]
        returns = pd.DataFrame()
        for i, f in enumerate(files):
            if int(f[:4]) < self.start_year:
                continue
            temp = pd.read_csv(os.path.join(ddir, f), index_col=0)
            # Adjustment for zero exposures on final day if present
            exposure_columns = [x for x in temp.columns
                                if x.find('Exposure') > -1]
            temp.loc[temp.index.max(), exposure_columns] = np.nan
            temp.fillna(method='pad', inplace=True)
            # Keep just long and shorts and combine
            unique_columns = set([int(x.split('_')[1]) for x in temp.columns])
            for cn in unique_columns:
                temp['LongRet_{}'.format(cn)] = \
                    temp['LongPL_{}'.format(cn)] / \
                    temp['Exposure_{}'.format(cn)]
                temp['ShortRet_{}'.format(cn)] = \
                    temp['ShortPL_{}'.format(cn)] / \
                    temp['Exposure_{}'.format(cn)]
            ret_columns = [x for x in temp.columns if x.find('Ret') > 0]
            temp = temp.loc[:, ret_columns]
            # Add to returns
            returns = returns.add(temp, fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.long_short_returns = returns

    def import_all_output(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('all_output') > 0]
        if len(files) == 0:
            print('No `all_output` files available to analyze')
            self.all_output = None
        # Trim files for test periods
        if self.test_periods > 0:
            files = files[:-self.test_periods]
        all_output = pd.DataFrame()
        for f in files:
            if int(f[:4]) < self.start_year:
                continue
            temp = pd.read_csv(os.path.join(ddir, f), index_col=0)
            # Add to returns
            all_output = all_output.add(temp, fill_value=0)

        all_output.index = convert_date_array(all_output.index)
        self.all_output = all_output

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

    def parameter_correlations(self, param, drop_params=None, plot=False):
        if not hasattr(self, 'returns'):
            self.import_return_frame()
        if not hasattr(self, 'column_params'):
            self.import_column_params()
        cparams = classify_params(self.column_params)
        if drop_params:
            cparams = filter_classified_params(cparams, drop_params)
        params = cparams[param]
        data = pd.DataFrame()
        keys = params.keys()
        # Sort to make it easy to read
        keys.sort()
        for key in keys:
            cols = params[key]
            temp = self.returns[cols].mean(axis=1).to_frame()
            temp.columns = [key]
            data = data.join(temp, how='outer')
        if plot:
            make_correlation_heatmap(data, title=param)
        else:
            return data.corr()

    # ~~~~~~ Notes ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_note(self, note, path=config.SIMULATION_OUTPUT_DIR):
        note_path = os.path.join(path, self.strategy_class,
                                 self.run_name, 'notes.json')
        if os.path.isfile(note_path):
            notes = json.load(open(note_path, 'r'))
        else:
            notes = {}
        now = dt.datetime.utcnow()
        notes[now.strftime('%Y-%m-%dT%H:%M:%S')] = note
        with open(note_path, 'w') as outfile:
            json.dump(notes, outfile)

    def get_notes(self, path=config.SIMULATION_OUTPUT_DIR):
        note_path = os.path.join(path, self.strategy_class,
                                 self.run_name, 'notes.json')
        if not os.path.isfile(note_path):
            return 'No notes files'
        notes = json.load(open(note_path, 'r'))
        out = pd.Series(notes).to_frame().reset_index()
        out.columns = ['DateTime', 'Note']
        out = out.sort_values('DateTime')
        out = out.reset_index(drop=True)
        return out

    def add_star(self, path=config.SIMULATION_OUTPUT_DIR):
        star_path = os.path.join(path, self.strategy_class,
                                 self.run_name, 'starred.json')
        now = dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        star = {'date_starred': now}
        with open(star_path, 'w') as outfile:
            json.dump(star, outfile)


###############################################################################

class RunManagerGCP(RunManager):

    def __init__(self, strategy_class, run_name, start_year=1950,
                 test_periods=6):
        super(RunManagerGCP, self).__init__(
            strategy_class, run_name, start_year, test_periods)
        self._gcp_client = storage.Client()
        self._bucket = self._gcp_client.get_bucket(
            config.GCP_STORAGE_BUCKET_NAME)

    # ~~~~~~ Viewing Available Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @staticmethod
    def get_strategies():
        gcp_client = storage.Client()
        bucket = gcp_client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
        all_files = list(bucket.list_blobs())
        all_simulation_files = [x for x in all_files if x.name[:5] == 'simul']
        all_simulations = set([x.name.split('/')[1]
                               for x in all_simulation_files])
        return [x for x in all_simulations if len(x) > 0]

    @staticmethod
    def get_run_names(strategy_class):
        gcp_client = storage.Client()
        bucket = gcp_client.get_bucket(config.GCP_STORAGE_BUCKET_NAME)
        all_files = list(bucket.list_blobs())
        # Get unique runs from StrategyClass blobs
        all_simulation_files = [x.name for x in all_files if x.name.find(
            'simulations/{}'.format(strategy_class)) >= 0]
        all_files_2 = [x.name for x in all_files]
        all_runs = list(set([x.split('/')[2] for x in all_simulation_files]))
        all_runs.sort()
        output = pd.DataFrame({'Run': all_runs, 'Description': np.nan,
                               'Starred': ''})
        for i, run in enumerate(all_runs):
            path = 'simulations/{}/{}/meta.json'.format(strategy_class, run)
            blob = bucket.get_blob(path)
            desc = json.loads(blob.download_as_string())
            output.loc[i, 'Description'] = desc['description']
            if 'completed' in desc:
                output.loc[i, 'Completed'] = desc['completed']
            else:
                output.loc[i, 'Completed'] = None
            if 'end_time' in desc:
                output.loc[i, 'RunDate'] = desc['end_time'][:10]
            elif 'start_time' in desc:
                output.loc[i, 'RunDate'] = desc['start_time'][:10]
            else:
                output.loc[i, 'RunDate'] = None
            # See if starred
            star_path = star_path = os.path.join(
                'simulations', strategy_class, run, 'starred.json')
            if star_path in all_files_2:
                output.loc[i, 'Starred'] = '*'
        return output[['Run', 'RunDate', 'Completed',
                       'Description', 'Starred']]

    # ~~~~~~ Import Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_storage_run_files(self, filter_text):
        base_path = os.path.join('simulations',
                                 self.strategy_class,
                                 self.run_name)
        all_files = [x.name for x in list(self._bucket.list_blobs())]
        all_files = [x for x in all_files if x.find(base_path) >= 0]
        all_files = [x for x in all_files if x.find(filter_text) >= 0]
        all_files.sort()
        return all_files

    def import_return_frame(self):
        all_files = self._get_storage_run_files('returns.csv')
        # Trim files for test periods
        if self.test_periods > 0:
            all_files = all_files[:-self.test_periods]
        returns = pd.DataFrame()
        for i, f in enumerate(all_files):
            if int(f.split('/')[-1][:4]) < self.start_year:
                continue
            blob = self._bucket.get_blob(f)
            data = pd.read_csv(StringIO(blob.download_as_string()),
                               index_col=0)
            returns = returns.add(data, fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.returns = returns

    def import_stats(self):
        files = self._get_storage_run_files('stats.json')
        self.stats = {}
        if files:
            for f in files:
                blob = self._bucket.get_blob(f)
                f_name = f.split('/')[-1]
                self.stats[f_name] = json.loads(blob.download_as_string())
        else:
            self.stats['20100101NOSTATS'] = {x: {'no_stat': -999} for x
                                             in self.column_params}

    def import_column_params(self):
        file_path = self._get_storage_run_files('column_params.json')[0]
        blob = self._bucket.get_blob(file_path)
        self.column_params = json.loads(blob.download_as_string())

    def import_meta(self):
        file_path = self._get_storage_run_files('meta.json')[0]
        blob = self._bucket.get_blob(file_path)
        self.meta = json.loads(blob.download_as_string())

    def import_long_short_returns(self):
        files = self._get_storage_run_files('all_output.csv')
        if len(files) == 0:
            print('No `all_output` files available to analyze')
            self.returns = None
        # Trim files for test periods
        if self.test_periods > 0:
            files = files[:-self.test_periods]
        returns = pd.DataFrame()
        for i, f in enumerate(files):
            if int(f.split('/')[-1][:4]) < self.start_year:
                continue
            blob = self._bucket.get_blob(f)
            temp = pd.read_csv(StringIO(blob.download_as_string()),
                               index_col=0)
            # Adjustment for zero exposures on final day if present
            exposure_columns = [x for x in temp.columns
                                if x.find('Exposure') > -1]
            temp.loc[temp.index.max(), exposure_columns] = np.nan
            temp.fillna(method='pad', inplace=True)
            # Keep just long and shorts and combine
            unique_columns = set([int(x.split('_')[1]) for x in temp.columns])
            for cn in unique_columns:
                temp['LongRet_{}'.format(cn)] = \
                    temp['LongPL_{}'.format(cn)] / \
                    temp['Exposure_{}'.format(cn)]
                temp['ShortRet_{}'.format(cn)] = \
                    temp['ShortPL_{}'.format(cn)] / \
                    temp['Exposure_{}'.format(cn)]
            ret_columns = [x for x in temp.columns if x.find('Ret') > 0]
            temp = temp.loc[:, ret_columns]
            # Add to returns
            returns = returns.add(temp, fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.long_short_returns = returns

    # ~~~~~~ Notes ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _import_notes(self):
        path = os.path.join('simulations', self.strategy_class,
                            self.run_name, 'notes.json')
        blob = self._bucket.get_blob(path)
        if blob:
            return json.loads(blob.download_as_string())
        else:
            return None

    def add_note(self, note):
        notes = self._import_notes()
        notes = notes if notes else {}
        #
        now = dt.datetime.utcnow()
        notes[now.strftime('%Y-%m-%dT%H:%M:%S')] = note
        path = os.path.join('simulations', self.strategy_class,
                            self.run_name, 'notes.json')
        blob = self._bucket.blob(path)
        blob.upload_from_string(json.dumps(notes))

    def get_notes(self):
        notes = self._import_notes()
        if not notes:
            return 'No notes files'
        out = pd.Series(notes).to_frame().reset_index()
        out.columns = ['DateTime', 'Note']
        out = out.sort_values('DateTime')
        out = out.reset_index(drop=True)
        return out

    def add_star(self):
        star_path = os.path.join('simulations', self.strategy_class,
                                 self.run_name, 'starred.json')
        now = dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        star = {'date_starred': now}
        blob = self._bucket.blob(star_path)
        blob.upload_from_string(json.dumps(star))


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


def get_quarterly_rets(data, column_name):
    data = data.copy()
    data['year'] = [d.year for d in data.index]
    data['qtr'] = [(d.month-1)/3 + 1 for d in data.index]
    data2 = data.groupby(['year', 'qtr'])[column_name].sum().reset_index()
    return data2.pivot(index='year', columns='qtr', values=column_name)


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


def get_run_data(strategy_name, cloud_flag):
    if cloud_flag:
        return RunManagerGCP.get_run_names(strategy_name)
    else:
        return RunManager.get_run_names(strategy_name)


def get_columns(param_dict):
    cols = []
    for vals1 in param_dict.values():
        for vals2 in vals1.values():
            cols.append(vals2)
    cols = list(set(sum(cols, [])))
    cols.sort()
    return cols


def make_correlation_heatmap(data, title=None):
    corr = data.corr()
    # Generate a mask for the upper triangle
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True
    plt.figure(figsize=(7, 6))
    cmap = sns.diverging_palette(11, 210, as_cmap=True)
    sns.heatmap(corr, mask=mask, cmap=cmap, vmin=-1.0, vmax=1.0, center=0,
                square=True, linewidths=1.5, cbar_kws={'shrink': 0.8, 'aspect': 50})
    if title:
        plt.title(title)
    plt.show()
