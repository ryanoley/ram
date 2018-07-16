import os
import json
import time
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
import cStringIO

from abc import ABCMeta, abstractmethod, abstractproperty

from StringIO import StringIO
from google.cloud import storage

# For plotting and writing to file. use('agg') is to disable display
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import matplotlib
    matplotlib.use('agg')
    import matplotlib.pyplot as plt

from ram import config
from ram.analysis.run_aggregator import RunAggregator
from gearbox import convert_date_array


class ModelSelection(object):

    __metaclass__ = ABCMeta

    params = {
        'training_freq': 'm',
        'training_periods': 12,
        'training_epochs': 1
    }

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def get_implementation_name(self):
        pass

    @abstractmethod
    def get_top_models(self):
        pass

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self,
                 write_flag=False,
                 checkpoint_n_epochs=10,
                 gcp_cloud_implementation=config.GCP_CLOUD_IMPLEMENTATION,
                 model_selection_output_dir=config.MODEL_SELECTION_OUTPUT_DIR
                 ):
        self._write_flag = write_flag
        self._checkpoint_n_epochs = checkpoint_n_epochs
        self._model_selection_dir = model_selection_output_dir
        self._cloud_flag = gcp_cloud_implementation

        if gcp_cloud_implementation:
            self._init_gcp_implementation()
        else:
            self._init_local_implementation()

        self._init_new_run()
        self._runs = RunAggregator()

    def _init_gcp_implementation(self):
        """
        Connect to GCP bucket
        """
        self._gcp_client = storage.Client()
        self._gcp_bucket = self._gcp_client.get_bucket(
            config.GCP_STORAGE_BUCKET_NAME)
        self._model_selection_dir = 'model_selection'
        return

    def _init_local_implementation(self):
        """
        Check if proper directory structure is in place
        """
        if not self._write_flag:
            return
        if not os.path.isdir(self._model_selection_dir):
            os.mkdir(self._model_selection_dir)

    def _init_new_run(self):
        self._create_output_dir()

    def _create_output_dir(self):
        """
        Sets the output_dir by incrementing run folder name
        """
        if not self._write_flag:
            return
        timestamp = dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')
        run_name = '{}_{}'.format(self.__class__.__name__, timestamp)
        self._run_name = run_name
        self._output_dir = self._model_selection_dir + '/' + run_name
        if not self._cloud_flag:
            os.mkdir(self._output_dir)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_run(self, run):
        self._runs.add_run(run)

    def start(self):
        # Import all runs and column parameters
        self._runs.aggregate_returns()
        self._raw_returns = self._runs.returns.copy()
        self._raw_column_params = self._runs.column_params.copy()
        del self._runs
        # If writing out results, setup
        if self._write_flag:
            self._write_new_run_output()
            print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print('Writing run as: {}'.format(self._run_name))
            print('Max date: {}'.format(self._raw_returns.index.max().date()))
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
            time.sleep(0.5)  # To make output look nice in Notebook
        self._create_results_objects(self._raw_returns)
        self._loop()

    def _loop(self):

        time_indexes = create_training_test_indexes(
            dates=self._raw_returns.index.unique(),
            training_freq=self.params['training_freq'],
            n_periods=self.params['training_periods'],
            )

        for ep in tqdm(range(self.params['training_epochs'])):

            for i, (train_dates, test_dates) in enumerate(time_indexes):
                # Penalize missing data points to keep aligned columns
                train_data = self._raw_returns.loc[train_dates].copy()
                train_data = train_data.fillna(-99)

                test_data = self._raw_returns.loc[test_dates].copy()
                test_data = test_data.fillna(0)
                # Search for top models. Model indexes can be multi-dim.
                model_indexes, scores = self.get_top_models(i, train_data)
                # Get test periods results
                self._process_results(i, test_data, model_indexes, scores)

            self._process_epoch_stats(ep)

    # ~~~~~~ Output Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _write_new_run_output(self):
        if not self._write_flag:
            return
        path1 = os.path.join(self._output_dir, 'raw_returns.csv')
        path2 = os.path.join(self._output_dir, 'raw_column_params.json')
        if self._cloud_flag:
            to_csv_cloud(self._raw_returns, path1, self._gcp_bucket)
            write_json_cloud(self._raw_column_params, path2, self._gcp_bucket)
        else:
            self._raw_returns.to_csv(path1)
            write_json(self._raw_column_params, path2)

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
            self.best_results_returns = pd.DataFrame(index=data.index,
                                                     dtype=np.float_)
            self.best_results_scores = {}
            self.best_results_column_indexes = {}
            self.epoch_stats = pd.DataFrame(columns=['Mean', 'Sharpe'])
        return

    def _process_results(self, time_index, test_data, model_indexes, scores):
        # Confirm dimensions
        assert isinstance(model_indexes, list)
        assert isinstance(model_indexes[0], list)
        assert isinstance(scores, list)
        assert len(model_indexes) == len(scores)
        # Check if model_indexes are already included
        if time_index in self.best_results_column_indexes:
            model_indexes, scores = clean_model_indexes(
                model_indexes, scores,
                self.best_results_column_indexes[time_index])
        if len(model_indexes) == 0:
            return

        # Sort everything by scores DESCENDING
        model_indexes = np.array(model_indexes)
        scores = np.array(scores)
        inds = np.argsort(-1 * scores)
        model_indexes = model_indexes[inds]
        scores = scores[inds]
        # Make returns from model_indexes
        returns = pd.DataFrame(index=test_data.index)
        for i, mods in enumerate(model_indexes):
            returns.loc[:, i] = test_data.iloc[:, mods].mean(axis=1)

        # Compare with old best results
        if time_index in self.best_results_scores:
            m_rets = self.best_results_returns.loc[test_data.index].copy()
            m_rets = m_rets.join(returns, rsuffix='N')
            # Append old and new scores/combs
            m_scores = np.append(self.best_results_scores[time_index], scores)
            m_combs = np.vstack((self.best_results_column_indexes[time_index],
                                 model_indexes))
            # Sort all
            best_inds = np.argsort(-m_scores)[:self.n_best_ports]
            # And select best returns
            m_rets = m_rets.iloc[:, best_inds]
            m_rets.columns = range(len(best_inds))
            # Write best results, including scores and columns
            self.best_results_returns.loc[m_rets.index] = m_rets
            self.best_results_scores[time_index] = m_scores[best_inds].tolist()
            self.best_results_column_indexes[time_index] = \
                m_combs[best_inds].tolist()

        else:
            # Join used here so it can recognize number of columns
            if self.best_results_returns.shape[1] == 0:
                for i in range(len(scores)):
                    self.best_results_returns.loc[:, i] = np.nan
            self.best_results_returns.loc[returns.index] = returns
            self.best_results_scores[time_index] = scores.tolist()
            self.best_results_column_indexes[time_index] = \
                model_indexes.tolist()

    def _process_epoch_stats(self, epoch_count):
        # Get row index to append
        i = self.epoch_stats.shape[0]
        stat1 = self.best_results_returns.mean()[0]
        stat2 = stat1 / self.best_results_returns.std()[0]
        self.epoch_stats.loc[i, :] = (stat1, stat2)

        # Checkpoint if nees to be written
        if (epoch_count % self._checkpoint_n_epochs == 0) and self._write_flag:

            scores = self.best_results_scores.copy()
            scores = {str(k): v for k, v in scores.iteritems()}

            indexes = self.best_results_column_indexes.copy()
            indexes = {str(k): v for k, v in indexes.iteritems()}

            # Output top params
            last_time_index = max(self.best_results_column_indexes.keys())
            best_indexes = self.best_results_column_indexes[last_time_index][0]

            best_indexes = self._raw_returns.columns[best_indexes]


            best_indexes = {r: self._raw_column_params[r]
                            for r in best_indexes}

            param_packet = {
                'datetime_created':
                    dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'model_params': best_indexes
            }

            # Plotted figure of best results
            plt.figure()
            plt.plot(self.best_results_returns.dropna().cumsum())
            plt.title('Best results')
            plt.grid()

            if self._cloud_flag:

                to_csv_cloud(self.epoch_stats, os.path.join(
                    self._output_dir, 'epoch_stats.csv'),
                    self._gcp_bucket)

                to_csv_cloud(self.best_results_returns, os.path.join(
                    self._output_dir, 'best_results_returns.csv'),
                    self._gcp_bucket)

                write_json_cloud(scores, os.path.join(
                    self._output_dir, 'best_results_scores.json'),
                    self._gcp_bucket)

                write_json_cloud(indexes, os.path.join(
                    self._output_dir, 'best_results_column_indexes.json'),
                    self._gcp_bucket)

                # Top params name should be versioned
                top_params_name = 'current_params_{}.json'.format(
                    self._run_name)
                write_json_cloud(param_packet, os.path.join(
                    self._output_dir, top_params_name),
                    self._gcp_bucket)

                # Matplotlib
                sio = cStringIO.StringIO()
                plt.savefig(sio, format='png')
                blob = self._gcp_bucket.blob(os.path.join(
                    self._output_dir, 'best_results.png'))
                blob.upload_from_string(sio.getvalue())

            else:
                self.epoch_stats.to_csv(os.path.join(
                    self._output_dir, 'epoch_stats.csv'))

                self.best_results_returns.to_csv(os.path.join(
                    self._output_dir, 'best_results_returns.csv'))

                write_json(scores, os.path.join(
                    self._output_dir, 'best_results_scores.json'))

                write_json(indexes, os.path.join(
                    self._output_dir, 'best_results_column_indexes.json'))

                top_params_name = 'params_{}.json'.format(self._run_name)
                write_json(param_packet, os.path.join(
                    self._output_dir, top_params_name))

                # Matplotlib plot
                plt.savefig(os.path.join(
                    self._output_dir, 'best_results.png'))

            # Flush matplotlib
            plt.close('all')
        return


# ~~~~~~ Training/Test Indexes ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def create_training_test_indexes(dates,
                                 training_freq='m',
                                 n_periods=0):

    """
    Creates a list of tuples.
    * The first entry is an array of training dates
    * The second entry is an array of test dates

    NOTE: The last entry in the list will have a training set
    that is the entire date series, and an empty test entry

    Parameters
    ----------
    dates : array
    training_freq : str
        Training frequency
    n_periods : int
        Number of periods to return in training data. If zero, returns
        growing training set
    """
    assert isinstance(dates, pd.DatetimeIndex)
    dates = dates.unique()

    if training_freq == 'w':
        # New period starts on the first trading day of the week
        weekdays = np.array([x.weekday() for x in dates])
        transition_indexes = np.where(np.diff(weekdays) < 0)[0] + 1

    elif training_freq == 'm':
        months = np.array([x.month for x in dates])
        transition_indexes = np.where(np.diff(months) != 0)[0] + 1

    else:
        # Get changes in quarters
        quarters = np.array([x.quarter for x in dates])
        transition_indexes = np.where(np.diff(quarters) != 0)[0] + 1

    # Add zero index and final index
    transition_indexes = np.append([0], transition_indexes)
    transition_indexes = np.append(transition_indexes, [dates.shape[0]])

    # Adjust loop starting point given period length
    if n_periods < 1:
        start_ind = 2
    else:
        start_ind = n_periods + 1

    output = []
    for i in range(start_ind, len(transition_indexes)):
        d1 = transition_indexes[i-n_periods-1]
        d2 = transition_indexes[i-1]
        d3 = transition_indexes[i]
        if n_periods < 1:
            d1 = 0
        output.append((dates[d1:d2], dates[d2:d3]))

    # Final entry is all dates and empty test. This is for generating the
    # current top parameters.
    # output.append((dates, []))

    return output


# ~~~~~~ Clean Indexes functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clean_model_indexes(model_indexes, scores, original):
    output1 = []
    output2 = []
    for i in range(len(model_indexes)):
        if model_indexes[i] not in original:
            output1.append(model_indexes[i])
            output2.append(scores[i])
    return output1, output2


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
