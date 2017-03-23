import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy


class BirdsStrategy(Strategy):

    def __init__(self, data_dir):
        self.univ_size = 500
        self.data_dir = data_dir

    def get_iter_index(self):
        return range(len(self._get_date_iterator()))

    def run_index(self, index):

        t_start, cut_date, t_end = self._get_date_iterator()[index]

        data = self._get_data(t_start, cut_date, t_end,
                              univ_size=self.univ_size)

        args1 = make_arg_iter(self.pairselector.get_iterable_args())

        args2 = make_arg_iter(self.constructor.get_iterable_args())

        ind = 0
        output_results = pd.DataFrame()
        output_params = {}
        output_stats = {}

        for a1 in args1:
            scores, pair_info = self.pairselector.get_best_pairs(
                data, cut_date, **a1)

            # Optimization
            self.constructor.set_and_prep_data(scores, pair_info, data)

            for a2 in args2:
                results, stats = self.constructor.get_daily_pl(**a2)

                results.columns = [ind]
                output_results = output_results.join(results, how='outer')
                temp_params = {}
                temp_params.update(a1)
                temp_params.update(a2)
                output_params[ind] = temp_params
                output_stats[ind] = stats
                ind += 1

        deliverable = {'returns': output_results,
                       'column_params': output_params,
                       'statistics': output_stats}

        return deliverable

    def _get_date_iterator(self):
        """
        Bookend dates for start training, start test (~eval date)
        and end test sets.
        """
        all_dates = self.datahandler.get_all_dates()
        all_dates = all_dates[all_dates >= dt.datetime(2007, 12, 1)]
        # Generate first dates of quarter
        qtrs = np.array([(d.month-1)/3 + 1 for d in all_dates])
        inds = np.append([True], np.diff(qtrs) != 0)
        # Add in final date from all dates available.
        inds[-1] = True
        quarter_dates = all_dates[inds]
        # Get train start, test start, and final quarter date
        iterable = zip(quarter_dates[:-5],
                       quarter_dates[4:-1],
                       quarter_dates[5:])
        return iterable

    def _get_data(self, start_date, filter_date, end_date, univ_size):
        """
        Makes the appropriate period's data.
        """
        # Adjust date by one day for filter date
        adj_filter_date = filter_date - dt.timedelta(days=1)

        filter_args = {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': univ_size}

        features = list(set(self.pairselector.get_feature_names() +
                            self.constructor.get_feature_names()))

        data = self.datahandler.get_filtered_univ_data(
            features=features,
            start_date=start_date,
            end_date=end_date,
            filter_date=adj_filter_date,
            filter_args=filter_args)

        data = data.drop_duplicates()
        data.SecCode = data.SecCode.astype(str)
        data = data.sort_values(['SecCode', 'Date'])

        return data

if __name__ == '__main__':

    strategy = BirdsStrategy()
    strategy.start()