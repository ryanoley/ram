import numpy as np
import pandas as pd
import datetime as dt

from ram.analysis.model_selection.model_selection import ModelSelection


class CombinationSearch(ModelSelection):

    # For randomly generated integers
    seed_ind = 1234
    n_best_ports = 10
    strats_per_port = 5

    def get_implementation_name(self):
        return 'CombinationSearch'

    def get_top_models(self, time_index, train_data):
        """
        Main selection mechanism of top combinations. Optimizes on Sharpe.
        """
        # Generates a bunch of random vectors of Ints that represent cols
        combs = self._generate_random_combs(
            train_data.shape[1], time_index)

        criteria = 'mean'
        # Calculate sharpes
        if criteria == 'sharpe':
            scores = self._get_sharpes(train_data, combs)

        elif criteria == 'mean':
            scores = self._get_means(train_data, combs)
        else:
            raise 'Criteria needs to be selected from: [sharpe, mean]'
        best_inds = np.argsort(-scores)[:self.n_best_ports]

        return combs[best_inds].tolist(), scores[best_inds].tolist()

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
