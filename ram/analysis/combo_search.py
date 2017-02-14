import os
import random
import numpy as np
import pandas as pd

from gearbox import read_csv


class CombinationSearch(object):

    def __init__(self):
        self.data = pd.DataFrame({}, index=pd.DatetimeIndex([]))
        self.data_labels = {}

    def add_data(self, new_data, frame_label):
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_data.index, pd.DatetimeIndex)
        # Map inputted data column names
        self.data_labels[frame_label] = new_data.columns
        self.data = self.data.join(new_data, how='outer')
        self.data.columns = range(self.data.shape[1])
        return

    def set_training_params(self, freq='m', n_periods=12,
                            n_ports_per_combo=5, n_best_combos=5):
        self._train_freq = freq
        self._train_n_periods = n_periods
        self._train_n_ports_per_combo = n_ports_per_combo
        self._train_n_best_combos = n_best_combos

    def start(self):
        assert self._train_freq
        assert self._train_n_periods

        self._clean_input_data()
        self._create_training_indexes()
        self._create_results_objects()

        try:
            self.seed_ind = 0
            while True:
                for t1, t2, t3 in self._time_indexes:
                    train_data = self.data.iloc[t1:t2]
                    test_data = self.data.iloc[t2:t3]
                    test_results, train_scores, combs = \
                        self._fit_top_combinations(
                            t2, train_data, test_data, self.seed_ind)
                    self._process_results(
                        t2, test_results, train_scores, combs)
                    self.seed_ind += 1

        except KeyboardInterrupt:
            pass
            # Write out everything so it can be restarted at same spot with
            # same everything.

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #  This is where the innovation happens

    def _fit_top_combinations(self, time_index, train_data,
                              test_data, seed_ind):
        random.seed(seed_ind)
        combs = self._generate_random_combs(time_index)
        # Calculate sharpes
        sharpes = self._get_sharpes(train_data, combs)
        best_inds = np.argsort(-sharpes)[:self._train_n_best_combos]
        test_results = pd.DataFrame(
            self._get_ports(test_data, combs[best_inds]).T,
            index=test_data.index)
        return test_results, sharpes[best_inds], combs[best_inds]

    def _get_sharpes(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1) / np.std(ports, axis=1)

    def _get_ports(self, data, combs):
        return np.mean(data.values.T[combs, :], axis=1)

    def _generate_random_combs(self):
        combs = np.array([
            random.sample(range(train_data.shape[1]),
                          self._train_n_ports_per_combo)
            for x in range(10000)])
        # Get unique values
        combs = np.sort(combs, axis=1)
        combs = np.vstack({tuple(row) for row in combs})

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _clean_input_data(self):
        ind = self.data.isnull() & self.data.fillna(method='pad').notnull()
        self.data.where(~ind, other=0, inplace=True)
        return

    def _create_training_indexes(self):
        if self._train_freq == 'm':
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.month for d in self.data.index]))[0] + 1
        else:
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.quarter for d in self.data.index]))[0] + 1
        transition_indexes = np.append([0], transition_indexes)
        transition_indexes = np.append(transition_indexes,
                                       [self.data.shape[0]])
        if self._train_n_periods < 1:
            # Grow infinitely
            self._time_indexes = zip(
                [0] * len(transition_indexes[1:-1]),
                transition_indexes[1:-1],
                transition_indexes[2:])
        else:
            n1 = self._train_n_periods
            # Fixed length period
            self._time_indexes = zip(
                transition_indexes[:-n1-1],
                transition_indexes[n1:-1],
                transition_indexes[n1+1:])
        return

    def _create_results_objects(self):
        self.best_results_rets = pd.DataFrame(
            columns=range(self._train_n_best_combos),
            index=self.data.index,
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
            best_inds = np.argsort(-m_scores)[:self._train_n_best_combos]
            m_rets = m_rets.iloc[:, best_inds]
            m_rets.columns = range(self._train_n_best_combos)
            self.best_results_rets.loc[m_rets.index] = m_rets
            self.best_results_scores[time_index] = m_scores[best_inds]
            self.best_results_combs[time_index] = m_combs[best_inds]

        else:
            # Simple insert
            self.best_results_rets.loc[test_rets.index] = test_rets
            self.best_results_scores[time_index] = scores
            self.best_results_combs[time_index] = combs
