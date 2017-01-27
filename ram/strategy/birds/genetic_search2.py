import numpy as np
import pandas as pd


class GeneticSearch(object):

    def __init__(self, ret_data, ests_long, ests_short):
        ##  Assert columns have proper rows in them
        assert np.all(pd.Series([
            'SecCode', 'Date', 'T_1', 'T_2', 'T_3',
            'ReturnDay1', 'ReturnDay2', 'ReturnDay3']).isin(
            ret_data.columns))
        assert ests_long.shape[0] == ests_short.shape[0]
        assert ests_long.shape[1] == ests_short.shape[1]
        assert ests_long.shape[0] == ret_data.shape[0]

        # Sort input data
        sort_inds = np.argsort(ret_data.Date, ret_data.SecCode).values
        ret_data = ret_data.iloc[sort_inds]
        ests_long = ests_long.iloc[sort_inds]
        ests_short = ests_short.iloc[sort_inds]

        self._format_ret_data(ret_data)
        self._format_estimate_data(ret_data, ests_long, False)
        self._format_estimate_data(ret_data, ests_short, True)

    def _format_ret_data(self, ret_data):
        """
        Format into numpy arrays that are quickly accessible via
        the reshaped_row_number
        """
        self.trading_dates = ret_data['T_1'].append(
            ret_data['T_2']).append(ret_data['T_3']).values

        sort_inds = np.argsort(self.trading_dates)

        self.trading_dates = self.trading_dates[sort_inds]
        self.daily_returns = ret_data['ReturnDay1'].append(
            ret_data['ReturnDay2']).append(
            ret_data['ReturnDay3']).values[sort_inds]

        ## TEMP
        seccodes = ret_data['SecCode'].values.tolist() * 3

        self.reshaped_row_number = sort_inds.reshape(3, len(ret_data)).T

    def _format_estimate_data(self, ret_data, ests, short=False):
        cols = ests.columns
        ests['SecCode'] = ret_data.SecCode
        ests['Date'] = ret_data.Date
        ests['RowNumber'] = range(len(ests))
        for c in cols:
            if c == cols[0]:
                ests2 = np.array([ests.pivot(
                    index='Date', columns='SecCode', values=c).values])
            else:
                ests2 = np.vstack((ests2, np.array([ests.pivot(
                    index='Date', columns='SecCode', values=c).values])))
        if short:
            self.ests_array_short = ests2
            self.row_nums_short = ests.pivot(
                index='Date', columns='SecCode', values='RowNumber').values
        else:
            self.ests_array_long = ests2
            self.row_nums_long = ests.pivot(
                index='Date', columns='SecCode', values='RowNumber').values

    @staticmethod
    def init_population(count, n_confs):
        """
        Create a number of individuals (i.e. a population).
        where the weights sum to zero (across rows)
        """
        weightsL = np.random.rand(count, n_confs)
        weightsL = weightsL / weightsL.sum(axis=1)[:, np.newaxis]
        weightsS = np.random.rand(count, n_confs)
        weightsS = weightsS / weightsS.sum(axis=1)[:, np.newaxis]
        return [(w1, w2) for w1, w2 in zip(weightsL, weightsS)]

    @staticmethod
    def _bucket_mean(x, y):
        """
        `x` MUST BE ORDERED ASCENDING!!
        """
        changes = np.append([-1], np.where(x[1:] != x[:-1])[0])
        counts = np.diff(np.append(changes, len(y)-1))
        means = np.add.reduceat(y, changes+1, dtype=np.float_) / counts
        uniq_x = x[changes+1]
        return uniq_x, counts, means


