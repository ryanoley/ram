import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import read_csv

from ram.strategy.base import Strategy
from ram.data.dh_sql import DataHandlerSQL

from ram.utils.statistics import create_strategy_report


class ReversionStrategy(Strategy):

    def __init__(self):
        self.datahandler = DataHandlerSQL()

    def get_results(self):
        return self.results

    def start(self):

        date_iter = self._get_date_iterator()

        pull_vars = ['LAG1_PRMA5_Close',
                     'LAG1_PRMA30_Close',
                     'LAG1_BOLL30_Close',
                     'LAG1_Close', 'LAG5_Close']

        sort_cols = ['LAG1_PRMA5_Close',
                     'LAG1_PRMA30_Close',
                     'LAG1_BOLL30_Close',
                     'LagRet']

        features = pull_vars + ['Vwap', 'Close']

        filter_args = {'univ_size': 400, 'where': 'MarketCap > 500'}

        for d1, d2, d3 in date_iter:

            data = self.datahandler.get_filtered_univ_data(
                features=features,
                start_date=d2,
                end_date=d3,
                filter_date=d1,
                filter_args=filter_args)

            # Construct return variables
            data['LagRet'] = data.LAG1_Close / data.LAG5_Close - 1

            # Entry and exit flags
            data['EntryFlag'] = (data.Date == d2).astype(int)
            data['ExitFlag'] = (data.Date == d3).astype(int)

            data['Ret'] = np.where(
                data.EntryFlag, data.Close / data.Vwap - 1,
                np.where(data.ExitFlag, data.Vwap / data.Close.shift(1) - 1,
                         data.Close / data.Close.shift(1) - 1))

            data['Ret2'] = np.where(
                data.EntryFlag, data.Close / data.LAG1_Close - 1,
                data.Close / data.Close.shift(1) - 1)

            rets = self._get_sort_port_rets(data, sort_cols, sort_date=d2)

            if d1 == date_iter[0][0]:
                results = rets
            else:
                results = results.append(rets)

        self.results = results

    def start_live(self):
        return -1

    ###########################################################################

    def _get_sort_port_rets(self, data, sort_cols, sort_date):

        cols = ['Ret1_{0}'.format(sc) for sc in sort_cols] + \
                ['Ret2_{0}'.format(sc) for sc in sort_cols]
        out = pd.DataFrame(columns=cols)

        for sc in sort_cols:
            sort_df = data[data.Date == sort_date].copy()
            sort_df = sort_df.sort_values(sc)

            # Get long IDs
            long_ids = sort_df.ID.iloc[:40].tolist()
            short_ids = sort_df.ID.iloc[-40:].tolist()

            long_data = data[data.ID.isin(long_ids)].copy()
            short_data = data[data.ID.isin(short_ids)].copy()

            # Costs - VWAP, then Close prices
            long_data.loc[long_data.EntryFlag == 1, 'Ret'] -= 0.00075
            short_data.loc[short_data.EntryFlag == 1, 'Ret'] += 0.00075
            long_data.loc[long_data.ExitFlag == 1, 'Ret'] -= 0.00075
            short_data.loc[short_data.ExitFlag == 1, 'Ret'] += 0.00075

            long_data.loc[long_data.EntryFlag == 1, 'Ret2'] -= 0.0003
            short_data.loc[short_data.EntryFlag == 1, 'Ret2'] += 0.0003
            long_data.loc[long_data.ExitFlag == 1, 'Ret2'] -= 0.0003
            short_data.loc[short_data.ExitFlag == 1, 'Ret2'] += 0.0003

            # Daily Rets
            rets = long_data.groupby('Date')['Ret'].mean() - \
                short_data.groupby('Date')['Ret'].mean()

            rets2 = long_data.groupby('Date')['Ret2'].mean() - \
                short_data.groupby('Date')['Ret2'].mean()

            out.loc[:, 'Ret1_{0}'.format(sc)] = rets
            out.loc[:, 'Ret2_{0}'.format(sc)] = rets2

        return out

    def _get_date_iterator(self):
        # Get weekly bookends for querying database
        #  Friday, Monday, Friday
        all_dates = self.datahandler.get_all_dates()
        dow = [d.weekday() for d in all_dates]
        filter_d = []
        start_d = []
        end_d = []
        for i in range(len(dow))[252:]:
            if (dow[i] == 0) & (i+4 < len(dow)):
                filter_d.append(all_dates[i-1])
                start_d.append(all_dates[i])
                end_d.append(all_dates[i+4])
        return zip(filter_d, start_d, end_d)


if __name__ == '__main__':

    strategy = ReversionStrategy()
    strategy.start()
