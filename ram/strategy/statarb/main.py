import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.data.dh_mongo import DataHandlerMongoDb
from ram.utils.statistics import create_strategy_report

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    def __init__(self):
        self.datahandler = DataHandlerMongoDb()
        self.pairselector = PairsStrategy1()
        self.constructor = PortfolioConstructor()

    def start(self):

        dates = self._get_date_iterator()

        output = pd.DataFrame(columns=['Ret'])

        for t_start, cut_date, t_end in dates:
            # COULD THIS ALL GO IN IT'S OWN FUNCTION FOR PARALLELIZATION?
            t_filter = cut_date - dt.timedelta(days=1)
            data, trade_data = self._get_data(
                t_start, t_filter, t_end,
                univ_size=100, data_cols=['Close'])

            z_scores = self.pairselector.get_best_pairs(
                data, cut_date, window=20)

            plexp = self.constructor.get_daily_pl(z_scores, trade_data)
            output = output.append(plexp.iloc[1:])

        self.results = output

    def get_results(self):
        pass

    def start_live(self):
        pass

    def _get_date_iterator(self):
        """
        Bookend dates for start training, start test (~eval date)
        and end test sets.

        Training data has one year's data, test is one quarter.

        NOTE:   Could this be put into the data handler at some point?
        """
        all_dates = self.datahandler.get_all_dates()
        # BUG: for some reason the filter doesn't work for earliest dates
        if len(all_dates) > 326:
            all_dates = all_dates[326:]
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

    def _get_data(self, start_date, filter_date, end_date,
                  univ_size, data_cols):
        """
        ## THIS SHOULD ALL BE PUT INTO DATA HANDLER

        Makes the appropriate period's data.

        Uses some instance params like data cols, univ size and filter col.
        """
        if not isinstance(data_cols, list):
            data_cols = [data_cols]
        # Additional data to pull
        pull_cols = list(set(
            data_cols +
            ['Close', 'AdjustFactor', 'ActualDividend', 'SplitFactor']
        ))

        # Make from bdh
        data = self.datahandler.get_filtered_univ_data(
            univ_size=400,
            features=pull_cols,
            start_date=start_date,
            end_date=end_date,
            filter_date=filter_date,
            filter_column='AvgDolVolume',
            filter_bad_ids=True)

        # Trading data - After filter date
        trade_data = data[['Date', 'ID', 'Close',
                           'SplitFactor', 'ActualDividend']].copy()
        trade_data = trade_data.rename(columns={'ActualDividend': 'Dividend'})
        trade_data.Dividend = trade_data.Dividend.fillna(0)
        # Add get split factors
        trade_data['SplitMultiplier'] = data['SplitFactor'] / \
            data['SplitFactor'].shift(1)
        trade_data['Close'] /= trade_data['SplitFactor']
        trade_data = trade_data[trade_data.Date > filter_date]

        # Creates TOTRET for all pricing columns. Split and dividend adjusted.
        # Adjust pricing columns open/high/low/close
        adj_cols = ['Open', 'High', 'Low', 'Close', 'VWAP']
        for col in data_cols:
            if col in adj_cols:
                data[col] = data[col] * data.AdjustFactor
        # Keep only requested data for training data
        data = data[['ID', 'Date'] + data_cols]

        return data, trade_data


if __name__ == '__main__':

    strategy = StatArbStrategy()
    strategy.start()
    path = '/Users/mitchellsuter/Desktop/'
    name = 'StatArbStrategy'
    create_strategy_report(strategy.get_results(), name, path)
