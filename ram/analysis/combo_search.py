import os
import json
import random
import numpy as np
import pandas as pd

from gearbox import convert_date_array


class CombinationSearch(object):

    def __init__(self, output_dir=None):
        self.output_dir = output_dir
        self.data = pd.DataFrame({}, index=pd.DatetimeIndex([]))
        self.data_labels = {}
        self.seed_ind = 0

    def add_data(self, new_data, frame_label):
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_data.index, pd.DatetimeIndex)
        # Map inputted data column names
        self.data_labels[frame_label] = new_data.columns
        self.data = self.data.join(new_data, how='outer', rsuffix='R')
        self.data.columns = range(self.data.shape[1])
        return

    def set_training_params(self, freq='m', n_periods=12,
                            n_ports_per_combo=5, n_best_combos=5,
                            long_short_combs=False):
        self._train_freq = freq
        self._train_n_periods = n_periods
        self._train_n_ports_per_combo = n_ports_per_combo
        self._train_n_best_combos = n_best_combos
        self._train_long_short_combs = long_short_combs

    def restart(self):
        self._load_comb_search_session()
        print 'Restarting Search'
        self._loop()

    def start(self):
        assert self._train_freq
        assert self._train_n_periods
        assert self._train_n_ports_per_combo
        assert self._train_n_best_combos
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
                            t2, train_data, test_data, self.seed_ind)

                    self._process_results(
                        t2, test_results, train_scores, combs)
                    self.seed_ind += 1
                self._write_results_output(False)

        except KeyboardInterrupt:
            self._write_results_output(True)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #  This is where the innovation happens

    def _fit_top_combinations(self, time_index, train_data,
                              test_data, seed_ind):
        random.seed(seed_ind)
        if self._train_long_short_combs:
            combs = self._generate_random_combs_long_short(
                train_data.shape[1], time_index)
            sharpes = self._get_sharpes_long_short(train_data, combs)
        else:
            combs = self._generate_random_combs(
                train_data.shape[1], time_index)
            # Calculate sharpes
            sharpes = self._get_sharpes(train_data, combs)

        best_inds = np.argsort(-sharpes)[:self._train_n_best_combos]

        if self._train_long_short_combs:
            combs1 = combs[:, :(combs.shape[1]/2)][best_inds]
            combs2 = combs[:, (combs.shape[1]/2):][best_inds]
            ports = self._get_ports(test_data, combs1) - \
                self._get_ports(test_data, combs2)
            test_results = pd.DataFrame(ports.T, index=test_data.index)

        else:
            test_results = pd.DataFrame(
                self._get_ports(test_data, combs[best_inds]).T,
                index=test_data.index)

        return test_results, sharpes[best_inds], combs[best_inds]

    def _get_sharpes(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1) / np.std(ports, axis=1)

    def _get_sharpes_long_short(self, data, combs):
        combs1 = combs[:, :(combs.shape[1]/2)]
        combs2 = combs[:, (combs.shape[1]/2):]
        ports = self._get_ports(data, combs1) - self._get_ports(data, combs2)
        return np.mean(ports, axis=1) / np.std(ports, axis=1)

    def _get_ports(self, data, combs):
        return np.mean(data.values.T[combs, :], axis=1)

    def _generate_random_combs(self, n_choices, time_index):
        combs = np.array([
            random.sample(range(n_choices),
                          self._train_n_ports_per_combo)
            for x in range(10000)])
        # Get unique values
        combs = np.sort(combs, axis=1)
        combs = np.vstack({tuple(row) for row in combs})
        # Make sure the combinations aren't in the current best
        if time_index in self.best_results_combs:
            current_combs = self.best_results_combs[time_index]
            for c in current_combs:
                inds = np.sum(combs == c, axis=1) != 5
                combs = combs[inds]
        return combs

    def _generate_random_combs_long_short(self, n_choices, time_index):
        combs = np.array([
            random.sample(range(n_choices),
                          self._train_n_ports_per_combo * 2)
            for x in range(10000)])
        # Split
        combs1 = combs[:, :(combs.shape[1]/2)]
        combs2 = combs[:, (combs.shape[1]/2):]
        # Sort
        combs1 = np.sort(combs1, axis=1)
        combs2 = np.sort(combs2, axis=1)
        # Put back into tuples
        combs = np.hstack((combs1, combs2))
        combs = np.vstack({tuple(row) for row in combs})
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

    def _write_init_output(self):
        if self.output_dir:
            if os.path.exists(self.output_dir):
                raise Exception('Cannot overwrite directory')
            os.makedirs(self.output_dir)
            # Write data
            self.data.to_csv(os.path.join(self.output_dir, 'master_data.csv'))
            # Write parameters
            params = {
                'train_freq': self._train_freq,
                'train_n_periods': self._train_n_periods,
                'train_n_ports_per_combo': self._train_n_ports_per_combo,
                'train_n_best_combos': self._train_n_best_combos}
            outpath = os.path.join(self.output_dir, 'params.json')
            with open(outpath, 'w') as outfile:
                json.dump(params, outfile)
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

                outpath = os.path.join(self.output_dir, 'seed.json')
                self._write_params(
                    {'seed_ind': np.array([self.seed_ind])}, outpath)
        return

    def _load_comb_search_session(self):
        if self.output_dir:
            # Load data
            self.data = self._read_csv(os.path.join(self.output_dir,
                                                    'master_data.csv'))
            # Load parameters
            outpath = os.path.join(self.output_dir, 'params.json')
            params = json.load(open(outpath, 'r'))
            self._train_freq = params['train_freq']
            self._train_n_ports_per_combo = params['train_n_ports_per_combo']
            self._train_n_periods = params['train_n_periods']
            self._train_n_best_combos = params['train_n_best_combos']
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
            outpath = os.path.join(self.output_dir, 'seed.json')
            self.seed_ind = json.load(open(outpath, 'r'))['seed_ind'][0]
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
