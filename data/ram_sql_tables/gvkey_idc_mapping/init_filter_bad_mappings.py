import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

STARTDATE = dt.date(1980, 1, 1)
ENDDATE = dt.date(2079, 1, 1)
NUMADJ = ['First', 'Second', 'Third', 'Fourth', 'Fifth']

DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping')


def main():

    df_gvkeys, df_idc, output = import_data()

    uniq_idc_codes = df_idc.Code.unique()
    if len(output):
        uniq_idc_codes = uniq_idc_codes[~pd.Series(uniq_idc_codes).isin(
            output.IdcCode.unique()).values]

    for i, idc in enumerate(uniq_idc_codes):
        if i < 4:
            continue

        print '{0} of {1}'.format(i, len(uniq_idc_codes))
        idc_data = df_idc[df_idc.Code == idc]
        gvkey_data = df_gvkeys[df_gvkeys.ShortCusip.isin(
            idc_data.Cusip.unique())]
        idc_obs = match_id_prompt(gvkey_data, idc_data)
        output = output.append(idc_obs)
        output.to_csv(os.path.join(DDIR, 'filtered_ids.csv'), index=False)


def match_id_prompt(gvkey_data, idc_data):
    idc = idc_data.Code.unique()[0]
    # Suggestions
    gvkey_suggestion, dates_suggestion = \
        _create_suggestions(gvkey_data, idc_data)

    while True:
        print '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
        print idc_data
        print '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
        print gvkey_data.sort_values(['GvKey', 'Changedate'])
        print '\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'

        n_gvkeys = raw_input('Number of GVKeys: ')

        try:
            n_gvkeys = int(n_gvkeys)
        except:
            print 'Enter integer for the number of GVKeys'
            continue

        if n_gvkeys == 1:
            response = _prompt_gvkey_selection(gvkey_suggestion)
            idc_obs = _create_row(idc, response, STARTDATE, ENDDATE)

        else:
            idc_obs = pd.DataFrame([])
            for i in range(n_gvkeys):
                # GVKey
                print '\n{0} GVKey'.format(NUMADJ[i])
                gv = _prompt_gvkey_selection(gvkey_suggestion)
                gvkey_suggestion = gvkey_suggestion[gvkey_suggestion != gv]

                # END DATE
                print '\n{0} EndDate'.format(NUMADJ[i])
                if i == (n_gvkeys-1):
                    ed = ENDDATE
                else:
                    ed = _prompt_date_selection(dates_suggestion)
                    dates_suggestion = dates_suggestion[dates_suggestion > ed]

                # START DATE
                if i == 0:
                    sd = STARTDATE
                else:
                    sd = next_stard_date
                next_stard_date = ed + dt.timedelta(days=1)
                idc_obs = idc_obs.append(_create_row(idc, gv, sd, ed))

        print idc_obs
        response = raw_input('Commit? [y]')
        if response == 'y':
            break


def _prompt_gvkey_selection(input_vals):
    print 'Options:'
    for i, x in enumerate(input_vals):
        print '[{0}]: {1}'.format(i+1, x)
    response = raw_input('Selection: ')
    try:
        response = int(response) - 1
        if response < 0 or response >= len(input_vals):
            print '\nERROR: Selection not available'
            return _prompt_gvkey_selection(input_vals)
        return input_vals[response]
    except:
        print '\nERROR: Input integer'
        return _prompt_gvkey_selection(input_vals)


def _prompt_date_selection(input_vals):
    if len(input_vals) > 0:
        print 'Date Options:'
        for i, x in enumerate(input_vals):
            print '[{0}]: {1}'.format(i+1, x)
        response = raw_input('Selection, press `x` to enter custom date: ')
    else:
        response = 'x'

    if response == 'x':
        date = raw_input('Custom Date YYYYmmdd: ')
        try:
            return dt.date(int(date[:4]), int(date[4:6]), int(date[6:8]))
        except:
            print '\nERROR: Improperly formatted'
            return _prompt_date_selection(input_vals)
    else:
        try:
            response = int(response) - 1
            if response < 0 or response >= len(input_vals):
                print '\nERROR: Selection not available'
                return _prompt_date_selection(input_vals)
            return input_vals[response]
        except:
            print '\nERROR: Input integer'
            return _prompt_date_selection(input_vals)


def _create_suggestions(gvkey_data, idc_data):
    gvkey_data = gvkey_data.sort_values(['Changedate'])
    gvkey_suggestions = gvkey_data.GvKey.unique()

    # Date suggestions are on rows where GvKey changes
    # Offset these by one day since we are prompting for end dates
    dates_suggestions = gvkey_data.Changedate[
        gvkey_data.GvKey.diff().abs() > 0].values
    dates_suggestions = [d - dt.timedelta(days=1) for d in dates_suggestions]
    # Append end dates from idc_data
    dates_suggestions = np.unique(np.append(dates_suggestions,
                                            idc_data.EndDate))
    return gvkey_suggestions, dates_suggestions


def _create_row(idccode, gvkey, start_date, end_date):
    tempdf = pd.DataFrame([])
    tempdf['IdcCode'] = [idccode]
    tempdf['GVKey'] = [gvkey]
    tempdf['StartDate'] = [start_date]
    tempdf['EndDate'] = [end_date]
    return tempdf


def import_data():
    # Import gvkey data
    df1 = pd.read_csv(os.path.join(DDIR, 'bad_gvkeydata.csv'),
                      header=0, skiprows=[1])
    df1.columns = [x.replace(' ', '') for x in df1.columns]
    df1.Changedate = convert_date_array(
        df1.Changedate.apply(lambda x: x[:10]))
    df1.MinReportDate = convert_date_array(
        df1.MinReportDate.apply(lambda x: x[:10]))
    df1.MaxReportDate = convert_date_array(
        df1.MaxReportDate.apply(lambda x: x[:10]))
    df1.Name = df1.Name.apply(lambda x: x.rstrip())
    df1.Ticker = df1.Ticker.apply(lambda x: x.rstrip())
    df1['ShortCusip'] = df1.Cusip.apply(lambda x: x[:8])
    df1 = df1.loc[:, ['GvKey', 'Changedate', 'MinReportDate', 'MaxReportDate',
                      'Name', 'Ticker', 'Cusip', 'ShortCusip']]
    # Import IDCData
    df2 = pd.read_csv(os.path.join(DDIR, 'bad_idcdata.csv'),
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
    # Already formatted work - EACH ITERATION WRITES TO FILE SO NO WORK IS LOST
    try:
        df3 = pd.read_csv(os.path.join(DDIR, 'filtered_ids.csv'), header=0)
    except:
        df3 = pd.DataFrame([])
    return df1, df2, df3


if __name__ == '__main__':
    main()
