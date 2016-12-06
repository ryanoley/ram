import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy


class YearEnd(Strategy):

    COST = 0.0015

    def get_iter_index(self, hold_per=5):
        return self._get_date_iterator()
    
    def run_index(self, index):
        pass

    def _get_date_iterator(self, hold_per=5):
        """
        Iterator with revlevant dates (eval_dt, entry_dt, exit_dt)
        for each event (year end)
        """
        db_dates = self.datahandler.get_all_dates()

        fdoy = pd.date_range('1996-01-01', '2016-01-01', freq='AS')
        fdoy = np.array([x.to_pydatetime() for x in fdoy])

        fbdoy =  np.array([db_dates[db_dates > x].min() for x in fdoy])
        lbdoy =  np.array([db_dates[db_dates < x].max() for x in fdoy])

        lbdix = np.array([np.where(db_dates == x)[0][0] for x in lbdoy])
        entryix = lbdix - hold_per
        evalix = entryix - 1
        
        # Get train start, test start, and final quarter date
        iterable = zip(db_dates[evalix], db_dates[entryix],
                       db_dates[lbdix])
        return iterable


    def _create_ix_features(self, eval_date, entry_date, exit_date,
                            hold_per=5, univ_size = 1000):
        '''
        Creates features for a single index of eval, entry and exit dates.
        Also takes parameters for the holding_period length and universe size.
        '''
        filter_args = {'filter': 'AvgDolVol',
                       'where': 'MarketCap >= 100 and Close_ >= 10',
                       'univ_size': univ_size}

        features = ['Vwap', 'LEAD{}_Vwap'.format(hold_per),
                    'LAG1_RClose', 'LAG1_MarketCap', 'LAG1_AvgDolVol',
                    'LAG1_PRMA30_Close', 'LAG1_PRMA60_Close',
                    'LAG1_PRMA120_Close', 'LAG1_PRMA250_Close',
                    'LAG1_BOLL30_Close', 'LAG1_BOLL60_Close',
                    'LAG1_BOLL120_Close', 'LAG1_BOLL250_Close',
                    'LAG1_RSI30_Close', 'LAG1_RSI60_Close',
                    'LAG1_RSI120_Close', 'LAG1_RSI250_Close',
                    'LAG1_DISCOUNT30_Close','LAG1_DISCOUNT60_Close',
                    'LAG1_DISCOUNT120_Close', 'LAG1_DISCOUNT250_Close',
                    'LAG1_VOL30_Close', 'LAG1_VOL60_Close',
                    'LAG1_VOL120_Close', 'LAG1_VOL250_Close',
                    'LAG1_MFI30_Close', 'LAG1_MFI60_Close',
                    'LAG1_MFI120_Close', 'LAG1_MFI250_Close', 'GSECTOR']

        df = self.datahandler.get_filtered_univ_data(
            features=features,
            start_date=entry_date,
            end_date=exit_date,
            filter_date=eval_date,
            filter_args=filter_args)
        df = df.loc[df.Date == entry_date]

        # Market Variables
        spy_features = ['Vwap', 'LEAD{}_Vwap'.format(hold_per),
                        'LAG1_PRMA30_Close', 'LAG1_PRMA60_Close',
                        'LAG1_PRMA120_Close', 'LAG1_PRMA250_Close']
        spy = self.datahandler.get_etf_data(
            ['SPY'],
            features=spy_features,
            start_date=entry_date,
            end_date=exit_date)
        spy = spy.loc[spy.Date == entry_date]

        #  Add Returns
        df['Ret'] = (df['LEAD{}_Vwap'.format(hold_per)] / df.Vwap) - 1
        df['RetH'] = ((df['LEAD{}_Vwap'.format(hold_per)] / df.Vwap) -
            float(spy['LEAD{}_Vwap'.format(hold_per)] / spy.Vwap))

        # Hedge Additional measures
        for col in spy_features[2:]:
            df[col] -= float(spy[col])
        
        # Rename columns
        df.rename(columns={x:x.split('_')[1] for x in features[2:-1]},
                  inplace=True)

        # Industry performance for selected variables
        avg_cols = ['PRMA30', 'PRMA60', 'PRMA120', 'PRMA250',
                    'DISCOUNT30', 'DISCOUNT60', 'DISCOUNT120', 'DISCOUNT250',
                    'RSI30', 'RSI60', 'RSI120', 'RSI250',
                    'VOL30', 'VOL60', 'VOL120', 'VOL250']
        ind_data = self._industry_avg(df.GSECTOR, df[avg_cols])
        df = df.merge(ind_data, left_index=True, right_index=True)
        
        # Rank 
        rank_cols = ['DISCOUNT30', 'DISCOUNT60', 'DISCOUNT120', 'DISCOUNT250']
        rank_data = self._rank_cols(df[rank_cols])
        df = df.merge(rank_data, left_index=True, right_index=True)

        return df.reset_index(drop=True)

    def _industry_avg(self, ind_series, base_data):
        '''
        Aggregate the series in avg_data over the unique values in ind_series.
        Attempts to quantify how an industry has performed as a whole.
        '''
        ind_data = pd.DataFrame(index = base_data.index)
        igrp = base_data.groupby(ind_series)
        igrps = igrp.groups

        for i in igrps.keys():
            iix = igrps[i]
            medians = base_data.loc[iix,:].median()

            for (col, m) in medians.iteritems():
                ind_data.loc[iix, 'Ind_{}'.format(col)] = m

        return ind_data

    def _rank_cols(self, base_data):
        '''
        Rank the series in base_data
        '''
        rank_data = pd.DataFrame(index = base_data.index)
        for col in base_data.columns:
            ranks = np.argsort(np.argsort(base_data[col]))
            rank_data.loc[:, 'Rank_{}'.format(col)] = ranks
        return rank_data

    def _create_univ_df(self, dt_iter, univ_size, hold_per):
        '''
        Loop through dt_iter and append results together.
        '''
        univ = pd.DataFrame([])

        for (dt_eval, dt_entry, dt_exit) in dt_iter:
            ix_df = self._create_ix_features(dt_eval, dt_entry, dt_exit,
                                             hold_per = hold_per,
                                             univ_size = univ_size)
            univ = univ.append(ix_df)
            print str(dt_exit)
        return univ.reset_index(drop=True)


if __name__ == '__main__':
        
    # PARAMS
    hold_per = 5
    univ_size = 1500

    strategy = YearEnd()
    dt_iter = strategy.get_iter_index(hold_per)
    univ = strategy._create_univ_df(dt_iter, univ_size, hold_per)

    univ = pd.read_csv('C:/temp/yearend.csv')


