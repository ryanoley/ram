import os
import shutil
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import ProgBar

from ram.data.data_handler_sql import DataHandlerSQL

OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'coverage_audit')

FEATURES = ['DISCOUNT126_Close', 'PRMA20_Close', 'PRMAH20_Open', 'AdjClose',
            'BOLL20_High', 'RSI10_Close', 'MFI10_Low', 'VOL20_Close',
            'AvgDolVol', 'MarketCap', 'GSECTOR', 'GGROUP', 'SI',
            'EARNINGSFLAG', 'SALESGROWTHQ',
            'EBITDAMARGIN', 'GROSSMARGINQ',
            'FREECASHFLOWQ', 'EBITGROWTHQ', 'OPERATINGINCOMEGROWTHQ']

UNIVSIZE = 1000


class DataTableCoverage(object):

    def __init__(self, history_flag):
        self.datahandler = DataHandlerSQL()
        all_dates = self._get_iterable_dates()
        if history_flag:
            if not os.path.isdir(OUTDIR):
                os.mkdir(OUTDIR)
            self.run(all_dates)
        else:
            self.run([all_dates[-1]])

    def run(self, date_iterator):
        for t1, t2 in ProgBar(date_iterator):
            path1 = os.path.join(OUTDIR, '{}_{}_seccodes.csv'.format(
                             t1.strftime('%Y%m%d'), t2.strftime('%Y%m%d')))
            path2 = os.path.join(OUTDIR, '{}_{}_variables.csv'.format(
                             t1.strftime('%Y%m%d'), t2.strftime('%Y%m%d')))
            if os.path.isfile(path1):
                continue
            data = self._get_data(t1, t2)
            # Get troublesome SecCodes
            data2 = data.isnull().copy().astype(int)
            data2['SecCode'] = data.SecCode
            data2 = data2.drop('Date', axis=1)
            data2 = data2.groupby('SecCode').mean().unstack().reset_index()
            data2.columns = ['Column', 'SecCode', 'MissingCoverage']
            bad_seccodes = data2[data2.MissingCoverage > .05]
            bad_seccodes.to_csv(path1, index=False)
            # Create means coverage for whole universe
            data = pd.DataFrame(data.isnull().mean()).T
            data = data.drop(['Date', 'SecCode'], axis=1)
            data.to_csv(path2, index=False)

    def _get_iterable_dates(self):
        """
        At this point it only grabs quarterly data
        """
        all_dates = self.datahandler.get_all_dates()
        all_dates = all_dates[all_dates >= dt.datetime(2000, 1, 1)]
        # Generate first dates of quarter
        qtrs = np.array([(d.month-1)/3 + 1 for d in all_dates])
        inds = np.append([True], np.diff(qtrs) != 0)
        # Add in final date from all dates available.
        inds[-1] = True
        quarter_dates = all_dates[inds]
        # Get train start, test start, and final quarter date
        iterable = zip(quarter_dates[:-4],
                       quarter_dates[4:])
        return iterable

    def _get_data(self, start_date, end_date):
        filter_args = {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': UNIVSIZE}
        df = self.datahandler.get_filtered_univ_data(
            features=FEATURES,
            start_date=start_date,
            end_date=end_date,
            filter_date=end_date,
            filter_args=filter_args)
        return df


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--history', action='store_true',
        help='Flag to rebuild full history')
    args = parser.parse_args()

    DataTableCoverage(args.history)
