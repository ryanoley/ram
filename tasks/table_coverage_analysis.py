import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import ProgBar

from ram.data.dh_sql import DataHandlerSQL

FEATURES = ['DISCOUNT126_Close', 'PRMA20_Close', 'PRMAH20_Open', 'AdjClose',
            'BOLL20_High', 'RSI10_Close', 'MFI10_Low', 'VOL20_Close',
            'AvgDolVol', 'MarketCap', 'GSECTOR', 'GGROUP', 'SI',
            'EARNINGSFLAG', 'ACCTSALESGROWTH',
            'ACCTEPSGROWTH', 'ACCTPRICESALES']

UNIVSIZE = 1000


class DataTableCoverage(object):

    def __init__(self):
        self.datahandler = DataHandlerSQL()

    def run(self):
        date_iterator = self._get_iterable_dates()
        for t1, t2 in ProgBar(date_iterator):
            data = self._get_data(t1, t2)
            # Get troublesome SecCodes
            data2 = data.isnull().copy().astype(int)
            data2['SecCode'] = data.SecCode
            data2 = data2.drop('Date', axis=1)
            data2 = data2.groupby('SecCode').mean().unstack().reset_index()
            data2.columns = ['Column', 'SecCode', 'MissingCoverage']
            bad_seccodes = data2[data2.MissingCoverage > .05]
            # Create means coverage for whole universe
            data = pd.DataFrame(data.isnull().mean()).T
            data['StartDate'] = t1
            data['EndDate'] = t2
            if t1 == date_iterator[0][0]:
                output = data
                output_bad_seccodes = bad_seccodes
            else:
                output = output.append(data)
                output_bad_seccodes = output_bad_seccodes.append(bad_seccodes)
        return output, output_bad_seccodes

    def _get_iterable_dates(self):
        """
        At this point it only grabs quarterly data
        """
        all_dates = self.datahandler.get_all_dates()
        all_dates = all_dates[all_dates >= dt.datetime(2016, 1, 1)]
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

    dtc = DataTableCoverage()
    variables, seccodes = dtc.run()

    output_path = os.path.join(
        os.getenv('DATA'), 'ram', 'data_audit_vars.csv')
    variables.to_csv(output_path, index=False)

    output_path = os.path.join(
        os.getenv('DATA'), 'ram', 'data_audit_seccodes.csv')
    seccodes.to_csv(output_path, index=False)
