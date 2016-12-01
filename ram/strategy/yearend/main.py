import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy



class YearEnd(Strategy):

    def get_iter_index(self, hold_per=5):
        return self._get_date_iterator()
    
    def run_index(self, index):
        pass

    def _get_date_iterator(self, hold_per=5):
        """
        Bookend dates for start training, start test (~eval date)
        and end test sets.

        Training data has one year's data, test is one quarter.
        """
        db_dates = self.datahandler.get_all_dates()

        fdoy = pd.date_range('1996-01-01', '2016-01-01', freq='AS')
        fdoy = np.array([x.to_pydatetime() for x in fdoy])
        
        fbdoy =  np.array([db_dates[db_dates > x].min() for x in fdoy])
        lbdoy =  np.array([db_dates[db_dates < x].max() for x in fdoy])
        
        lbdix = np.array([np.where(db_dates == x)[0][0] for x in lbdoy])
        entryix = lbdix - hold_per
        evalix = lbdix - hold_per - 1
        
        # Get train start, test start, and final quarter date
        iterable = zip(db_dates[evalix], db_dates[entryix],
                       db_dates[lbdix])
        return iterable


    def _create_ix_features(self, entry_date, eval_date, exit_date,
                            hold_per=5, univ_size = 400):
        
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
            univ_size=univ_size,
            filter_date=eval_date)

        df.rename(columns={x:x.split('_')[1] for x in features[2:-1]},
                  inplace=True)

        #  Filter using gap measure and momentum measure
        df = df.loc[df.Date == entry_date]
        df['Ret'] = (df['LEAD{}_Vwap'.format(hold_per)] / df.Vwap) - 1

        # Add in additional vars for industry performance
        avg_cols = ['DISCOUNT250', 'PRMA30']
        ind_data = self._industry_avg(df.GSECTOR, df[avg_cols])

        # Hedged Returns
        mkt = self._get_spy_rets(entry_date, exit_date, hold_per)
        df['RetH'] = (df['Ret'] - float(mkt.MktRet))
        
        return df.reset_index(drop=True)


    def _industry_avg(self, ind_series, avg_data):
        '''
        This function will aggregate the data in av_data over the unique
        values in the ind_series.  Idea is basicall how has an entire industry
        performed over various measures.
        '''
        #TODO
        return

    def _get_spy_rets(self, dt_entry, dt_exit, hold_per=5):
        spy = pd.DataFrame([])
        features = ['Vwap', 'LEAD{}_Vwap'.format(hold_per)]

        spy = self.datahandler.get_etf_data(['SPY'], features, dt_entry,
            dt_exit)
        spy = spy.loc[spy.Date == dt_entry]

        spy['MktRet'] = (spy['LEAD{}_Vwap'.format(hold_per)] / spy.Vwap) - 1

        return spy.reset_index(drop=True)


    def _create_univ_df(self, dt_iter, univ_size, hold_per):
        univ = pd.DataFrame([])

        for (dt_eval, dt_entry, dt_exit) in dt_iter:
            ix_df = self._create_ix_features(dt_entry, dt_eval, dt_exit,
                                             hold_per = hold_per,
                                             univ_size = univ_size)
            univ = univ.append(ix_df)
            print str(dt_exit)
        
        return univ.reset_index(drop=True)


if __name__ == '__main__':
        
    # PARAMS
    COST = 0.0015
    hold_per = 5
    univ_size = 1500
    
    strategy = YearEnd()
    dt_iter = strategy.get_iter_index(hold_per)
    univ = strategy._create_univ_df(dt_iter, univ_size, hold_per)
    

