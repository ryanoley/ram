import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array


# Helpers
start_date = dt.date(1980, 1, 1)
end_date = dt.date(2079, 1, 1)
TEXT = ['first', 'second', 'third', 'fourth', 'fifth']

DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping')
DDIR = '/Users/mitchellsuter/Desktop'


def main():

    df1, df2, output = import_data()

    uniq_idc_codes = df2.Code.unique()
    if len(output):
        uniq_idc_codes = uniq_idc_codes[~pd.Series(uniq_idc_codes).isin(
            output.IdcCode.unique()).values]

    try:
        for i, idc in enumerate(uniq_idc_codes):
            print '{0} of {1}'.format(i, len(uniq_idc_codes))
            idc_data = df2[df2.Code == idc]
            gvkey_data = df1[df1.ShortCusip.isin(idc_data.Cusip.unique())]
            # Suggestions
            gvkey_data = gvkey_data.sort_values(['GvKey', 'Changedate'])
            gvkey_suggestions = gvkey_data.GvKey.unique()

            # Date suggestions are on rows where GvKey changes
            dates_suggestions_start = gvkey_data.Changedate[
                gvkey_data.GvKey.diff().abs() > 0].values
            dates_suggestions_end = [
                z - dt.timedelta(days=1) for z in dates_suggestions_start]
            dates_suggestions_start = np.append(start_date,
                                                dates_suggestions_start)

            while True:
                print '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
                print idc_data
                print '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
                print gvkey_data
                print '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'

                try:
                    n_gvkeys = int(raw_input('Number of GVKeys: '))
                except:
                    print 'Enter correct number of gvkeys'
                    continue

                idc_obs = pd.DataFrame([])
                for i in range(n_gvkeys):
                    # GVKEY
                    response = raw_input('{0} GVKey {1}? [y, n]: '.format(
                        TEXT[i], gvkey_suggestions[i]))
                    if response == 'n':
                        gv = raw_input('Enter {0} GVKey: '.format(TEXT[i]))
                        if int(gv) not in gvkey_suggestions:
                            print 'GVKey not in data'
                            break
                    else:
                        gv = gvkey_suggestions[i]

                    # START DATE
                    if i == 0:
                        sd = start_date
                    else:
                        response = raw_input('{0} StartDate {1}? [y, n]: '.format(
                            TEXT[i], dates_suggestions_start[i]))
                        if response == 'n':
                            sd = raw_input('Enter {0} StartDate: '.format(TEXT[i]))
                        else:
                            sd = dates_suggestions_start[i]

                    # END DATE
                    if i == (n_gvkeys-1):
                        ed = end_date
                    else:
                        response = raw_input('{0} EndDate {1}? [y, n]: '.format(
                            TEXT[i], dates_suggestions_end[i]))
                        if response == 'n':
                            ed = raw_input('Enter {0} EndDate: '.format(TEXT[i]))
                        else:
                            ed = dates_suggestions_end[i]
                    new_row = create_row(idc, gv, sd, ed)
                    idc_obs = idc_obs.append(new_row)

                print idc_obs
                response = raw_input('Commit? [y]')

                if response == 'y':
                    break
            output = output.append(idc_obs)
            output.to_csv(os.path.join(DDIR, 'filtered_ids.csv'), index=False)

    except KeyboardInterrupt:
        output.to_csv(os.path.join(DDIR, 'filtered_ids.csv'), index=False)


def import_data():

    # Import gvkey data
    df1 = pd.read_csv(os.path.join(DDIR, 'gvkeydata.csv'),
                      header=0, skiprows=[1])
    df1.columns = [x.replace(' ', '') for x in df1.columns]

    df1.Changedate = convert_date_array(df1.Changedate.apply(lambda x: x[:10]))
    df1.MinReportDate = convert_date_array(df1.MinReportDate.apply(lambda x: x[:10]))
    df1.MaxReportDate = convert_date_array(df1.MaxReportDate.apply(lambda x: x[:10]))
    df1.Name = df1.Name.apply(lambda x: x.rstrip())
    df1.Ticker = df1.Ticker.apply(lambda x: x.rstrip())
    df1['ShortCusip'] = df1.Cusip.apply(lambda x: x[:8])
    df1 = df1.loc[:, ['GvKey', 'Changedate', 'MinReportDate', 'MaxReportDate',
                      'Name', 'Ticker', 'Cusip', 'ShortCusip']]

    # Import IDCData
    df2 = pd.read_csv(os.path.join(DDIR, 'idcdata.csv'),
                      header=0, skiprows=[1])
    df2.columns = [x.replace(' ', '') for x in df2.columns]
    df2 = df2.loc[:, ['Code', 'StartDate', 'EndDate', 'Cusip', 'Ticker',
                      'Issuer', 'Exchange']]
    df2.StartDate = convert_date_array(df2.StartDate.apply(lambda x: x[:10]))
    df2.EndDate = convert_date_array(df2.EndDate.apply(lambda x: x[:10]))
    df2.Cusip = df2.Cusip.apply(lambda x: x.rstrip())
    df2.Ticker = df2.Ticker.apply(lambda x: x.rstrip())
    df2.Issuer = df2.Issuer.apply(lambda x: x.rstrip())
    df2.Exchange = df2.Exchange.apply(lambda x: x.rstrip())

    # Already formatted work
    try:
        df3 = pd.read_csv(os.path.join(DDIR, 'filtered_ids.csv'), header=0)
    except:
        df3 = pd.DataFrame([])

    return df1, df2, df3


def create_row(idccode, gvkey, start_date, end_date):
    tempdf = pd.DataFrame([])
    tempdf['IdcCode'] = [idccode]
    tempdf['GVKey'] = [gvkey]
    tempdf['StartDate'] = [start_date]
    tempdf['EndDate'] = [end_date]
    return tempdf


if __name__ == '__main__':
    main()
