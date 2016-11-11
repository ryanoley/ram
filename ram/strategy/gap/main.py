import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import read_csv

from ram.strategy.base import Strategy


# PARAMS
z = 1.
COST = 0.0015


class GapStrategy(Strategy):

    def get_iter_index(self):
        return range(len(self._get_date_iterator()))

    def run_index(self, index):

        t_start, t_end = self._get_date_iterator()[index]

        data = self._create_features(t_start, t_end)

        output = pd.DataFrame(0, columns=['R1', 'R2', 'R3'],
                              index=pd.date_range(start=t_start, end=t_end))

        # Returns
        up_data = data.loc[(data.zUp > 0) & (data.GapUp > 0)].copy()
        down_data = data.loc[(data.zDown < 0) & (data.GapDown < 0)].copy()

        # Costs
        up_data.Ret = up_data.Ret + COST
        down_data.Ret = down_data.Ret - COST

        # Sum all trades
        up_rets = up_data.groupby('Date').Ret.mean()
        down_rets = down_data.groupby('Date').Ret.mean()

        output.loc[up_rets.index, 'R1'] = up_rets * -1
        output.loc[down_rets.index, 'R2'] = down_rets

        output['R3'] = output.R1 + output.R2

        return output


    ###########################################################################

    def _create_features(self, start_date, end_date, z=1):

        univ_size = 400
        features = ['Open', 'High', 'Low', 'Close',
                    'LAG1_High', 'LAG1_Low',
                    'LAG1_VOL90_Close', 'LAG1_MA20_Close']

        df = self.datahandler.get_filtered_univ_data(
            features=features,
            start_date=start_date,
            end_date=end_date,
            univ_size=univ_size,
            filter_date=start_date)

        # AdjCloseMA20 and StdevRet are offset in sql file
        df['Ret'] = (df.Close - df.Open) / df.Open

        df['GapDown'] = (df.Open - df.LAG1_Low) / df.LAG1_Low
        df['GapUp'] = (df.Open - df.LAG1_High) / df.LAG1_High

        df['zUp'] = df.GapUp / df.LAG1_VOL90_Close
        df['zDown'] = df.GapDown / df.LAG1_VOL90_Close

        #  Filter using gap measure and momentum measure
        df = df.loc[
            ((df.Open <= df.LAG1_MA20_Close) & (df.zUp >= z)) |
            ((df.Open >= df.LAG1_MA20_Close) & (df.zDown <= -z))]
        df.sort_values('Date', inplace=True)

        return df.reset_index(drop=True)

    def _get_date_iterator(self):
        """
        Bookend dates for start training, start test (~eval date)
        and end test sets.

        Training data has one year's data, test is one quarter.
        """
        all_dates = self.datahandler.get_all_dates()
        # Arbitrary start date
        if len(all_dates) > 326:
            all_dates = all_dates[326:]
        # Generate first dates of quarter
        qtrs = np.array([(d.month-1)/3 + 1 for d in all_dates])
        inds = np.append([True], np.diff(qtrs) != 0)
        # Add in final date from all dates available.
        inds[-1] = True
        quarter_dates = all_dates[inds]

        # Get train start, test start, and final quarter date
        iterable = zip(quarter_dates[:-1],
                       quarter_dates[1:])
        return iterable


if __name__ == '__main__':

    import pdb; pdb.set_trace()
    strategy = GapStrategy()
    strategy.start()
