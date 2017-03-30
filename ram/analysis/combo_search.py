import numpy as np
import pandas as pd
import datetime as dt


class CombinationSearch(object):

    def __init__(self):
        # Default parameters
        self.params = {
            'train_freq': 'm',
            'n_periods': 12,
            'strats_per_port': 10,
            'n_best_ports': 5,
            'seed_ind': 0,
        }

    def attach_data(self, data):
        assert isinstance(data, pd.DataFrame)
        assert isinstance(data.index, pd.DatetimeIndex) or \
            isinstance(data.index[0], dt.date)
        self.column_labels = data.columns.tolist()
        data.columns = range(data.shape[1])
        self.data = data

    def start(self):
        self._create_results_objects()
        self._create_training_indexes()
        while True:
            for t1, t2, t3 in self._time_indexes:
                train_data = self.data.iloc[t1:t2].copy()
                # Drop any column that has an na
                train_data = train_data.T.dropna().T
                test_data = self.data.iloc[t2:t3].copy()
                # Same columns
                test_data = test_data[train_data.columns]
                # Flip signs on trade
                train_data = train_data.join(-1*train_data, rsuffix='_flip')
                test_data = test_data.join(-1*test_data, rsuffix='_flip')

                test_results, train_scores, combs = \
                    self._fit_top_combinations(
                        t2, train_data, test_data)
                self._process_results(
                    t2, test_results, train_scores, combs)
            print 'Single pass complete'

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _fit_top_combinations(self, time_index, train_data, test_data):

        combs = self._generate_random_combs(
            train_data.shape[1], time_index)
        # Calculate sharpes
        scores = self._get_sharpes(train_data, combs)
        best_inds = np.argsort(-scores)[:self.params['n_best_ports']]
        test_results = pd.DataFrame(
            self._get_ports(test_data, combs[best_inds]).T,
            index=test_data.index)
        return test_results, scores[best_inds], combs[best_inds]

    def _get_sharpes(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1) # / np.std(ports, axis=1)

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

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _create_training_indexes(self):
        if self.params['train_freq'] == 'm':
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.month for d in self.data.index]))[0] + 1
        elif self.params['train_freq'] == 'w':
            # 0s are mondays so get the day it goes from 4 to 0
            transition_indexes = np.where(np.diff(
                [d.weekday() for d in self.data.index]) < 0)[0] + 1
        else:
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.quarter for d in self.data.index]))[0] + 1
        transition_indexes = np.append([0], transition_indexes)
        transition_indexes = np.append(transition_indexes,
                                       [self.data.shape[0]])

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

    def _create_results_objects(self):
        if not hasattr(self, 'best_results_rets'):
            self.best_results_rets = pd.DataFrame(
                columns=range(self.params['n_best_ports']),
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
