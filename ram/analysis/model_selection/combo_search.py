import numpy as np
import pandas as pd
import datetime as dt

from ram.analysis.model_selection.model_selection import ModelSelection


class CombinationSearch(ModelSelection):

    # For randomly generated integers
    seed_ind = 1234
    n_best_ports = 10
    strats_per_port = 5

    def set_selection_criteria(self, criteria='mean'):
        self.criteria = criteria

    def get_implementation_name(self):
        return 'CombinationSearch'

    def get_top_models(self, time_index, train_data):
        """
        Main selection mechanism of top combinations. Optimizes on Sharpe.
        """
        # Generates a bunch of random vectors of Ints that represent cols
        combs = self._generate_random_combs(
            train_data.shape[1], time_index)

        if not hasattr(self, 'criteria'):
            self.set_selection_criteria()

        if self.criteria == 'sharpe':
            scores = self._get_sharpes(train_data, combs)

        elif self.criteria == 'mean':
            scores = self._get_means(train_data, combs)

        elif self.criteria == 'min_sharpe':
            min_scores = self._get_mins(train_data, combs)
            sharpe_scores = self._get_sharpes(train_data, combs)
            r1 = np.argsort(np.argsort(min_scores))
            r2 = np.argsort(np.argsort(sharpe_scores))
            scores = r1 + r2

        elif self.criteria == 'min_mean_sharpe':
            min_scores = self._get_mins(train_data, combs)
            sharpe_scores = self._get_sharpes(train_data, combs)
            mean_scores = self._get_means(train_data, combs)
            r1 = np.argsort(np.argsort(min_scores))
            r2 = np.argsort(np.argsort(sharpe_scores))
            r3 = np.argsort(np.argsort(mean_scores))
            scores = r1 + r2 + r3

        elif self.criteria == 'min_mean_sharpe_dd':
            min_scores = self._get_mins(train_data, combs)
            sharpe_scores = self._get_sharpes(train_data, combs)
            mean_scores = self._get_means(train_data, combs)
            dd_scores = self._get_draw_downs(train_data, combs)
            r1 = np.argsort(np.argsort(min_scores))
            r2 = np.argsort(np.argsort(sharpe_scores))
            r3 = np.argsort(np.argsort(mean_scores))
            r4 = np.argsort(np.argsort(dd_scores))
            scores = r1 + r2 + r3 + r4

        elif self.criteria == 'mean_min':
            mean_scores = self._get_means(train_data, combs)
            min_scores = self._get_mins(train_data, combs)
            r1 = np.argsort(np.argsort(min_scores))
            r2 = np.argsort(np.argsort(mean_scores))
            scores = r1 + r2

        elif self.criteria == 'mean_dd':
            mean_scores = self._get_means(train_data, combs)
            dd_scores = self._get_draw_downs(train_data, combs)
            r1 = np.argsort(np.argsort(dd_scores))
            r2 = np.argsort(np.argsort(mean_scores))
            scores = r1 + r2

        elif self.criteria == 'sharpe_dd':
            sharpe_scores = self._get_sharpes(train_data, combs)
            dd_scores = self._get_draw_downs(train_data, combs)
            r1 = np.argsort(np.argsort(dd_scores))
            r2 = np.argsort(np.argsort(sharpe_scores))
            scores = r1 + r2

        else:
            raise ValueError()

        best_inds = np.argsort(-scores)[:self.n_best_ports]

        return combs[best_inds].tolist(), scores[best_inds].tolist()

    def _get_sharpes(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1) / np.std(ports, axis=1)

    def _get_means(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.mean(ports, axis=1)

    def _get_mins(self, data, combs):
        ports = self._get_ports(data, combs)
        return np.min(ports, axis=1)

    def _get_draw_downs(self, data, combs):
        ports = self._get_ports(data, combs)
        cum_sum = np.cumsum(ports, axis=1)
        cum_max = np.maximum.accumulate(cum_sum, axis=1)
        return np.min(cum_sum - cum_max, axis=1)

    def _get_ports(self, data, combs):
        return np.mean(data.values.T[combs, :], axis=1)

    def _generate_random_combs(self, n_choices, time_index):
        # Set seed and update
        np.random.seed(self.seed_ind)
        self.seed_ind += 1
        combs = np.random.randint(
            0, high=n_choices, size=(10000, self.strats_per_port))
        # Sort items in each row, then sort rows
        combs = np.sort(combs, axis=1)
        combs = combs[np.lexsort([combs[:, i] for i in
                                  range(combs.shape[1]-1, -1, -1)])]
        # Drop repeats in same row
        combs = combs[np.sum(np.diff(combs, axis=1) == 0, axis=1) == 0]
        # Drop repeat rows
        combs = combs[np.append(True, np.sum(
            np.diff(combs, axis=0), axis=1) != 0)]

        return combs
