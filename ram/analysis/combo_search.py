import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
import cStringIO

from StringIO import StringIO
from google.cloud import storage

# For plotting and writing to file. use('agg') is to disable display
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

from gearbox import convert_date_array

from ram import config
from ram.analysis.run_aggregator import RunAggregator


class CombinationSearch(object):

    def __init__(self,
                 write_flag=False,
                 checkpoint_n_epochs=10,
                 combo_search_output_dir=config.COMBO_SEARCH_OUTPUT_DIR,
                 restart_combo_name=None):

        self.write_flag = write_flag
        self.checkpoint_n_epochs = checkpoint_n_epochs
        self._combo_search_output_dir = combo_search_output_dir
        self._init_gcp_implementation()
        self._init_params(restart_combo_name)
        # Output related functionality
        self.runs = RunAggregator()

    def _init_params(self, restart_combo_name=None):
        if restart_combo_name:
            self._restart_flag = True
            self.combo_run_dir = os.path.join(self._combo_search_output_dir,
                                              restart_combo_name)
        else:
            self._restart_flag = False
            # Default parameters
            self.params = {
                'train_freq': 'm',
                'n_periods': 12,
                'strats_per_port': 5,
                'n_best_ports': 5,
                'seed_ind': 1234
            }

    def _init_gcp_implementation(self):
        self._gcp_implementation = config.GCP_CLOUD_IMPLEMENTATION
        if self._gcp_implementation:
            self._gcp_client = storage.Client()
            self._gcp_bucket = self._gcp_client.get_bucket(
                config.GCP_STORAGE_BUCKET_NAME)
            self._combo_search_output_dir = 'combo_search'
        return

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_run(self, run):
        self.runs.add_run(run)

    def start(self, epochs=20, criteria='sharpe'):
        # Import all runs
        self.runs.aggregate_returns()
        # If writing out results, setup
        if self.write_flag:
            self._create_output_dir()
            self._init_output()
        self._create_results_objects(self.runs.returns)
        self._loop(epochs, criteria)

    def restart(self, epochs=20, criteria='sharpe'):
        self._init_restart()
        self._loop(epochs, criteria)

    def _loop(self, epochs, criteria):
        self._create_training_indexes(self.runs.returns)
        for ep in tqdm(range(epochs)):
            for t1, t2, t3 in self._time_indexes:
                # Penalize missing data points to keep aligned columns
                train_data = self.runs.returns.iloc[t1:t2].copy()
                train_data = train_data.fillna(-99)
                test_data = self.runs.returns.iloc[t2:t3].copy()
                test_data = test_data.fillna(0)
                # Search
                test_results, train_scores, combs = self._fit_top_combinations(
                    t2, train_data, test_data, criteria)
                self._process_results(t2, test_results, train_scores, combs)
            self._process_epoch_stats(ep)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _fit_top_combinations(self, time_index, train_data, test_data,
                              criteria='mean'):
        """
        Main selection mechanism of top combinations. Optimizes on Sharpe.
        """
        # Generates a bunch of random vectors of Ints that represent cols
        combs = self._generate_random_combs(
            train_data.shape[1], time_index)
        # Calculate sharpes
        if criteria == 'sharpe':
            scores = self._get_sharpes(train_data, combs)

        elif criteria == 'mean':
            scores = self._get_means(train_data, combs)
        else:
            raise 'Criteria needs to be selected from: [sharpe, mean]'
        best_inds = np.argsort(-scores)[:self.params['n_best_ports']]
        test_results = pd.DataFrame(
            self._get_ports(test_data, combs[best_inds]).T,
            index=test_data.index)
        return test_results, scores[best_inds], combs[best_inds]

    def _get_sharpes(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1) / np.std(ports, axis=1)

    def _get_means(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1)

    def _get_ports(self, data, combs):
        return np.mean(data.values.T[combs, :], axis=1)

    def _generate_random_combs(self, n_choices, time_index):
        # Set seed and update
        np.random.seed(self.params['seed_ind'])
        self.params['seed_ind'] += 1
        combs = np.random.randint(
            0, high=n_choices, size=(10000, self.params['strats_per_port']))
        # Sort items in each row, then sort rows
        combs = np.sort(combs, axis=1)
        combs = combs[np.lexsort([combs[:, i] for i in
                                  range(combs.shape[1]-1, -1, -1)])]
        # Drop repeats in same row
        combs = combs[np.sum(np.diff(combs, axis=1) == 0, axis=1) == 0]
        # Drop repeat rows
        combs = combs[np.append(True, np.sum(
            np.diff(combs, axis=0), axis=1) != 0)]
        # Make sure the combinations aren't in the current best
        if time_index in self.best_results_combs:
            current_combs = self.best_results_combs[time_index]
            for c in current_combs:
                inds = (np.sum(combs == c, axis=1) !=
                        self.params['strats_per_port'])
                combs = combs[inds]
        return combs

    # ~~~~~~ Time Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _create_training_indexes(self, data):
        """
        Creates the row indexes that create the training and test data sets.
        Used as an .iloc[] on the data frame.

        Parameters
        ----------
        data : pd.DataFrame
            Uses dates from index to get the row numbers for the bookends
            for the training and test data.
        """
        if self.params['train_freq'] == 'm':
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.month for d in data.index]))[0] + 1
        elif self.params['train_freq'] == 'w':
            # 0s are mondays so get the day it goes from 4 to 0
            transition_indexes = np.where(np.diff(
                [d.weekday() for d in data.index]) < 0)[0] + 1
        else:
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.quarter for d in data.index]))[0] + 1
        transition_indexes = np.append([0], transition_indexes)
        transition_indexes = np.append(transition_indexes,
                                       [data.shape[0]])
        if self.params['n_periods'] < 1:
            # Grow infinitely
            self._time_indexes = zip(
                [0] * len(transition_indexes[1:-1]),
                transition_indexes[1:-1],
                transition_indexes[2:])
        else:
            n1 = self.params['n_periods']
            # Fixed length period
            self._time_indexes = zip(
                transition_indexes[:-n1-1],
                transition_indexes[n1:-1],
                transition_indexes[n1+1:])
        return

    # ~~~~~~ Output Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _create_output_dir(self):
        """
        Creates the directory structure for the output AND CRUCIALLY
        sets the run_dir. This implementation has been reworked for gcp.
        """
        if self.write_flag and self._gcp_implementation:
            all_files = [x.name for x in self._gcp_bucket.list_blobs()]
            all_files = [x for x in all_files if x.startswith(
                self._combo_search_output_dir)]
            strip_str = self._combo_search_output_dir + '/'
            all_files = [x.replace(strip_str, '') for x in all_files]
            all_files = [x for x in all_files if x.find('combo_run') >= 0]
            if all_files:
                new_ind = max([int(x.split('/')[0].strip('combo_run_'))
                               for x in all_files]) + 1
            else:
                new_ind = 1
        elif os.path.isdir(self._combo_search_output_dir):
            all_dirs = [x for x in os.listdir(
                self._combo_search_output_dir) if x[:9] == 'combo_run']
            new_ind = max([int(x.strip('combo_run_')) for x in all_dirs]) + 1 \
                if all_dirs else 1
        elif self.write_flag:
            os.makedirs(self._combo_search_output_dir)
            new_ind = 1
        else:
            new_ind = 1
        # Get all run versions for increment for this run
        self.combo_run_dir = os.path.join(self._combo_search_output_dir,
                                          'combo_run_{0:04d}'.format(new_ind))
        if self.write_flag and not self._gcp_implementation:
            if not os.path.isdir(self.combo_run_dir):
                os.mkdir(self.combo_run_dir)

    def _init_output(self):
        path1 = os.path.join(self.combo_run_dir, 'all_returns.csv')
        path2 = os.path.join(self.combo_run_dir, 'all_column_params.json')
        if self.write_flag and self._gcp_implementation:
            to_csv_cloud(self.runs.returns, path1, self._gcp_bucket)
            write_json_cloud(self.runs.column_params, path2, self._gcp_bucket)
        elif self.write_flag and not self._gcp_implementation:
            self.runs.returns.to_csv(path1)
            write_json(self.runs.column_params, path2)
        else:
            pass

    def _create_results_objects(self, data):
        """
        Creates containers to hold the daily returns for the best portfolios,
        and also some dictionaries that specify the "scores" and which
        columns from the data the combinations came from.

        Parameters
        ----------
        data : pd.DataFrame
            Used simply to get the dates
        """
        if not hasattr(self, 'best_results_rets'):
            self.best_results_rets = pd.DataFrame(
                columns=range(self.params['n_best_ports']),
                index=data.index,
                dtype=np.float_)
            self.best_results_scores = {}
            self.best_results_combs = {}
            self.epoch_stats = pd.DataFrame(columns=['Mean', 'Sharpe'])
        return

    def _process_results(self, time_index, test_rets, scores, combs):

        if time_index in self.best_results_combs:
            m_rets = self.best_results_rets.loc[test_rets.index].copy()
            m_rets = m_rets.join(test_rets, rsuffix='N')
            m_scores = np.append(self.best_results_scores[time_index], scores)
            m_combs = np.vstack((self.best_results_combs[time_index], combs))
            best_inds = np.argsort(-m_scores)[:self.params['n_best_ports']]
            m_rets = m_rets.iloc[:, best_inds]
            m_rets.columns = range(self.params['n_best_ports'])
            self.best_results_rets.loc[m_rets.index] = m_rets
            self.best_results_scores[time_index] = m_scores[best_inds]
            self.best_results_combs[time_index] = m_combs[best_inds]

        else:
            # Simple insert
            self.best_results_rets.loc[test_rets.index] = test_rets
            self.best_results_scores[time_index] = scores
            self.best_results_combs[time_index] = combs

    def _process_epoch_stats(self, epoch_count):
        # Get row index to append
        i = self.epoch_stats.shape[0]
        stat1 = self.best_results_rets.mean()[0]
        stat2 = stat1 / self.best_results_rets.std()[0]
        self.epoch_stats.loc[i, :] = (stat1, stat2)
        # Checkpoint if nees to be written
        if (epoch_count % self.checkpoint_n_epochs == 0) and self.write_flag:

            scores = self.best_results_scores.copy()
            scores = {str(k): list(v) for k, v in scores.iteritems()}

            combs = self.best_results_combs.copy()
            combs = {str(k): v.tolist() for k, v in combs.iteritems()}

            # Output top params
            last_time_index = max(self.best_results_combs.keys())
            best_combs = self.best_results_combs[last_time_index][0]
            best_combs = self.runs.returns.columns[best_combs]
            best_combs = {r: self.runs.column_params[r] for r in best_combs}

            # Plotted figure of best results
            plt.figure()
            plt.plot(self.best_results_rets.dropna().cumsum())
            plt.title('Best results')
            plt.grid()

            if self._gcp_implementation:
                to_csv_cloud(self.epoch_stats, os.path.join(
                    self.combo_run_dir, 'epoch_stats.csv'),
                    self._gcp_bucket)
                to_csv_cloud(self.best_results_rets, os.path.join(
                    self.combo_run_dir, 'best_results_rets.csv'),
                    self._gcp_bucket)

                write_json_cloud(scores, os.path.join(
                    self.combo_run_dir, 'best_results_scores.json'),
                    self._gcp_bucket)
                write_json_cloud(combs, os.path.join(
                    self.combo_run_dir, 'best_results_combs.json'),
                    self._gcp_bucket)
                write_json_cloud(best_combs, os.path.join(
                    self.combo_run_dir, 'current_top_params.json'),
                    self._gcp_bucket)
                # This is re-written because seed_ind is constantly updated
                write_json_cloud(self.params, os.path.join(
                    self.combo_run_dir, 'combo_search_params.json'),
                    self._gcp_bucket)
                # Matplotlib
                sio = cStringIO.StringIO()
                plt.savefig(sio, format='png')
                blob = self._gcp_bucket.blob(os.path.join(
                    self.combo_run_dir, 'best_results.png'))
                blob.upload_from_string(sio.getvalue())

            else:
                self.epoch_stats.to_csv(os.path.join(
                    self.combo_run_dir, 'epoch_stats.csv'))
                self.best_results_rets.to_csv(os.path.join(
                    self.combo_run_dir, 'best_results_rets.csv'))
                write_json(scores, os.path.join(
                    self.combo_run_dir, 'best_results_scores.json'))
                write_json(combs, os.path.join(
                    self.combo_run_dir, 'best_results_combs.json'))
                write_json(best_combs, os.path.join(
                    self.combo_run_dir, 'current_top_params.json'))
                # This is re-written because seed_ind is constantly updated
                write_json(self.params, os.path.join(
                    self.combo_run_dir, 'combo_search_params.json'))
                # Matplotlib plot
                plt.savefig(os.path.join(
                    self.combo_run_dir, 'best_results.png'))
            # Flush matplotlib
            plt.close('all')
        return

    def _init_restart(self):
        path1 = os.path.join(self.combo_run_dir, 'all_returns.csv')
        path2 = os.path.join(self.combo_run_dir, 'epoch_stats.csv')
        path3 = os.path.join(self.combo_run_dir, 'best_results_rets.csv')

        path4 = os.path.join(self.combo_run_dir, 'best_results_scores.json')
        path5 = os.path.join(self.combo_run_dir, 'best_results_combs.json')
        path6 = os.path.join(self.combo_run_dir, 'combo_search_params.json')
        path7 = os.path.join(self.combo_run_dir, 'all_column_params.json')

        if self._gcp_implementation:
            self.runs.returns = read_csv_cloud(path1, self._gcp_bucket)
            self.epoch_stats = read_csv_cloud(path2, self._gcp_bucket)
            self.best_results_rets = read_csv_cloud(path3, self._gcp_bucket)

            self.best_results_scores = read_json_cloud(path4, self._gcp_bucket)
            self.best_results_combs = read_json_cloud(path5, self._gcp_bucket)
            self.params = read_json_cloud(path6, self._gcp_bucket)
            self.runs.column_params = read_json_cloud(path7, self._gcp_bucket)
        else:
            self.runs.returns = pd.read_csv(path1)
            self.epoch_stats = pd.read_csv(path2)
            self.best_results_rets = pd.read_csv(path3)

            self.best_results_scores = read_json(path4)
            self.best_results_combs = read_json(path5)
            self.params = read_json(path6)
            self.runs.column_params = read_json(path7)

        # Process csv files with dates in indexes
        self.runs.returns = format_dataframe(self.runs.returns, True)
        self.epoch_stats = format_dataframe(self.epoch_stats)
        self.best_results_rets = format_dataframe(self.best_results_rets, True)
        # Combs and scores must have integer keys and numpy arrays
        # in values
        self.best_results_scores = {
            int(x): np.array(y) for x, y
            in self.best_results_scores.iteritems()}
        self.best_results_combs = {
            int(x): np.array(y) for x, y
            in self.best_results_combs.iteritems()}


# ~~~~~~ Read/Write functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def write_json(out_dictionary, path):
    assert isinstance(out_dictionary, dict)
    with open(path, 'w') as outfile:
        json.dump(out_dictionary, outfile)


def read_json(path):
    return json.load(open(path, 'r'))


def write_json_cloud(out_dictionary, path, bucket):
    assert isinstance(out_dictionary, dict)
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(out_dictionary))


def read_json_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return json.loads(blob.download_as_string())


def read_csv_cloud(path, bucket):
    blob = bucket.get_blob(path)
    return pd.read_csv(StringIO(blob.download_as_string()))


def to_csv_cloud(data, path, bucket):
    blob = bucket.blob(path)
    blob.upload_from_string(data.to_csv())


def format_dataframe(data, date_index=False):
    data = data.set_index(data.columns[0])
    del data.index.name
    if date_index:
        data.index = convert_date_array(data.index)
    return data
