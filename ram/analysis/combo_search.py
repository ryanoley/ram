import os
import json
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm

from ram.analysis.run_aggregator import RunAggregator


class CombinationSearch(object):

    def __init__(self, output_dir=None, checkpoint_n_epochs=10):
        # Default parameters
        self.params = {
            'train_freq': 'm',
            'n_periods': 12,
            'strats_per_port': 5,
            'n_best_ports': 5,
            'seed_ind': 1234
        }
        self.runs = RunAggregator()
        self.output_dir = os.path.join(output_dir, 'combo_search') if \
            output_dir else None
        self.checkpoint_n_epochs = checkpoint_n_epochs

    def add_run(self, run):
        self.runs.add_run(run)

    def start(self, epochs=20, criteria='sharpe'):
        # Merge
        self.runs.aggregate_returns()
        self._create_output_dir()
        self._create_results_objects(self.runs.returns)
        self._create_training_indexes(self.runs.returns)
        self._create_epoch_stat_objects()
        for ep in tqdm(range(epochs)):
            for t1, t2, t3 in self._time_indexes:
                # Penalize missing data points to keep aligned columns
                train_data = self.runs.returns.iloc[t1:t2].copy()
                train_data = train_data.fillna(-99)
                test_data = self.runs.returns.iloc[t2:t3].copy()
                test_data = test_data.fillna(0)
                # Search
                test_results, train_scores, combs = \
                    self._fit_top_combinations(
                        t2, train_data, test_data, criteria)
                self._process_results(
                    t2, test_results, train_scores, combs)
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
                inds = np.sum(combs == c, axis=1) != 5
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
        if self.output_dir:
            os.mkdir(self.output_dir)
            self.runs.returns.to_csv(os.path.join(self.output_dir,
                                                  'returns.csv'))
            with open(os.path.join(self.output_dir,
                                   'column_params.json'), 'w') as f:
                json.dump(self.runs.column_params, f)

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

    def _create_epoch_stat_objects(self):
        if not hasattr(self, 'epoch_stats'):
            self.epoch_stats = pd.DataFrame(columns=['Mean', 'Sharpe'])
        return

    def _process_epoch_stats(self, epoch_count):
        i = self.epoch_stats.shape[0]
        stat1 = self.best_results_rets.mean()[0]
        stat2 = stat1 / self.best_results_rets.std()[0]
        self.epoch_stats.loc[i, :] = (stat1, stat2)
        if (epoch_count % self.checkpoint_n_epochs == 0) & \
                (self.output_dir is not None):
            self.epoch_stats.to_csv(os.path.join(
                self.output_dir, 'epoch_stats.csv'))
            self.best_results_rets.to_csv(os.path.join(
                self.output_dir, 'best_results_rets.csv'))
            scores = self.best_results_scores.copy()
            scores = {str(k): list(v) for k, v in scores.iteritems()}
            with open(os.path.join(self.output_dir,
                                   'best_results_scores.json'), 'w') as f:
                json.dump(scores, f)
            combs = self.best_results_combs.copy()
            combs = {str(k): v.tolist() for k, v in combs.iteritems()}
            with open(os.path.join(self.output_dir,
                                   'best_results_combs.json'), 'w') as f:
                json.dump(combs, f)
            with open(os.path.join(self.output_dir, 'params.json'), 'w') as f:
                json.dump(self.params, f)
            # Output top params
            last_time_index = max(self.best_results_combs.keys())
            best_combs = self.best_results_combs[last_time_index][0]
            best_combs = self.runs.returns.columns[best_combs]
            best_combs = {r: self.runs.column_params[r] for r in best_combs}
            with open(os.path.join(self.output_dir,
                                   'current_top_params.json'), 'w') as f:
                json.dump(best_combs, f)
        return
