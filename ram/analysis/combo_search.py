import os
import json
import numpy as np
import pandas as pd

from gearbox import convert_date_array


class CombinationSearch(object):

    def __init__(self, output_dir=None):
        self.output_dir = output_dir
        self.data = pd.DataFrame({}, index=pd.DatetimeIndex([]))
        self.data_labels = {}
        # Default parameters
        self.params = {
            'freq': 'm',
            'n_periods': 12,
            'n_ports_per_combo': 5,
            'n_best_combos': 5,
            'seed_ind': 0
        }

    def add_data(self, new_data, frame_label):
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_data.index, pd.DatetimeIndex)
        # Map inputted data column names
        self.data_labels[frame_label] = new_data.columns
        self.data = self.data.join(new_data, how='outer', rsuffix='R')
        self.data.columns = range(self.data.shape[1])
        return

    def set_training_params(self, **kwargs):
        for key, value in kwargs.iteritems():
            self.params[key] = value

    def restart(self):
        self._load_comb_search_session()
        print 'Restarting Search'
        self._loop()

    def start(self):
        self._clean_input_data()
        self._create_results_objects()
        self._write_init_output()
        print 'Starting New Combination Search'
        self._loop()

    def _loop(self):
        self._create_training_indexes()
        try:
            while True:
                for t1, t2, t3 in self._time_indexes:

                    train_data = self.data.iloc[t1:t2]
                    test_data = self.data.iloc[t2:t3]

                    test_results, train_scores, combs = \
                        self._fit_top_combinations(
                            t2, train_data, test_data)

                    self._process_results(
                        t2, test_results, train_scores, combs)

                self._write_results_output(False)

        except KeyboardInterrupt:
            self._write_results_output(True)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #  This is where the innovation happens

    def _fit_top_combinations(self, time_index, train_data, test_data):

        combs = self._generate_random_combs(
            train_data.shape[1], time_index)

        # Calculate sharpes
        scores = self._get_sharpes(train_data, combs)

        best_inds = np.argsort(-scores)[:self.params['n_best_combos']]

        test_results = pd.DataFrame(
            self._get_ports(test_data, combs[best_inds]).T,
            index=test_data.index)

        return test_results, scores[best_inds], combs[best_inds]

    def _get_sharpes(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1) / np.std(ports, axis=1)

    def _get_ports(self, data, combs):
        return np.mean(data.values.T[combs, :], axis=1)

    def _generate_random_combs(self, n_choices, time_index):
        # Set seed and update
        np.random.seed(self.params['seed_ind'])
        self.params['seed_ind'] += 1

        combs = np.random.randint(
            0, high=n_choices, size=(10000, self.params['n_ports_per_combo']))

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

    def _clean_input_data(self):
        ind = self.data.isnull() & self.data.fillna(method='pad').notnull()
        self.data.where(~ind, other=0, inplace=True)
        return

    def _create_training_indexes(self):
        if self.params['freq'] == 'm':
            # Get changes in months
            transition_indexes = np.where(np.diff(
                [d.month for d in self.data.index]))[0] + 1
        elif self.params['freq'] == 'w':
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
        self.best_results_rets = pd.DataFrame(
            columns=range(self.params['n_best_combos']),
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
            best_inds = np.argsort(-m_scores)[:self.params['n_best_combos']]
            m_rets = m_rets.iloc[:, best_inds]
            m_rets.columns = range(self.params['n_best_combos'])
            self.best_results_rets.loc[m_rets.index] = m_rets
            self.best_results_scores[time_index] = m_scores[best_inds]
            self.best_results_combs[time_index] = m_combs[best_inds]

        else:
            # Simple insert
            self.best_results_rets.loc[test_rets.index] = test_rets
            self.best_results_scores[time_index] = scores
            self.best_results_combs[time_index] = combs

    def _write_init_output(self):
        if self.output_dir:
            if os.path.exists(self.output_dir):
                raise Exception('Cannot overwrite directory')
            os.makedirs(self.output_dir)
            # Write data
            self.data.to_csv(os.path.join(self.output_dir, 'master_data.csv'))
            # Write parameters
            outpath = os.path.join(self.output_dir, 'params.json')
            with open(outpath, 'w') as outfile:
                json.dump(self.params, outfile)
            outfile.close()
            self._write_results_output(True)
        return

    def _write_results_output(self, write_params=False):

        try:
            print 'Population mean score: {0}'.format(
                np.concatenate(self.best_results_scores.values()).mean())
        except:
            pass

        if self.output_dir:
            self.best_results_rets.to_csv(os.path.join(self.output_dir,
                                                       'best_returns.csv'))
            if write_params:
                outpath = os.path.join(self.output_dir, 'best_scores.json')
                self._write_params(self.best_results_scores, outpath)

                outpath = os.path.join(self.output_dir, 'best_combs.json')
                self._write_params(self.best_results_combs, outpath)
        return

    def _load_comb_search_session(self):
        if self.output_dir:
            # Load data
            self.data = self._read_csv(os.path.join(self.output_dir,
                                                    'master_data.csv'))
            # Load parameters
            outpath = os.path.join(self.output_dir, 'params.json')
            params = json.load(open(outpath, 'r'))
            self.set_training_params(**params)

            # Best results data
            self.best_results_rets = self._read_csv(
                os.path.join(self.output_dir, 'best_returns.csv'))

            outpath = os.path.join(self.output_dir, 'best_scores.json')
            self.best_results_scores = json.load(open(outpath, 'r'))
            self.best_results_scores = {
                int(key): np.array(vals) for key, vals in
                self.best_results_scores.iteritems()}

            outpath = os.path.join(self.output_dir, 'best_combs.json')
            self.best_results_combs = json.load(open(outpath, 'r'))
            self.best_results_combs = {
                int(key): np.array(vals) for key, vals in
                self.best_results_combs.iteritems()}
        return

    @staticmethod
    def _read_csv(path):
        data = pd.read_csv(path, index_col=None)
        data.columns = range(-1, data.shape[1]-1)
        data.loc[:, -1] = convert_date_array(data.loc[:, -1])
        data = data.set_index(-1)
        data.index = data.index.to_datetime()
        data.index.name = None
        return data

    @staticmethod
    def _write_params(params, path):
        outparams = {key: vals.tolist() for key, vals
                     in params.iteritems()}
        with open(path, 'w') as outfile:
            json.dump(outparams, outfile)
        outfile.close()
