import os
import sys
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

STARTDATE = dt.date(1960, 1, 1)
ENDDATE = dt.date(2079, 1, 1)
NUMADJ = ['First', 'Second', 'Third', 'Fourth', 'Fifth']

DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping')
DDIR = '/Users/mitchellsuter/Desktop/gvkey_mapping'


def start_manual_filter():
    # Import data
    df_gvkeys = import_bad_gvkeydata()
    df_idc = import_idccodes_to_handle()
    output = import_temp_filtered_ids()
    output_ignored = import_temp_filtered_ids_ignored()
    # Get unique IDC Codes to iterate
    uniq_idc_codes = df_idc[
        ~df_idc.Code.isin(output.IdcCode.unique())].Code.unique()
    for i, idc in enumerate(uniq_idc_codes):
        print '!!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
        print '!!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
        print '{0} of {1}'.format(i, len(uniq_idc_codes))
        idc_data = df_idc[df_idc.Code == idc]
        gvkey_data = df_gvkeys[df_gvkeys.ShortCusip.isin(
            idc_data.Cusip.unique())]
        idc_obs, ignored_obs = match_id_prompt(gvkey_data, idc_data)
        output = output.append(idc_obs)
        output_ignored = output_ignored.append(ignored_obs)
        # Format
        output.IdcCode = output.IdcCode.astype(int)
        output.GVKey = output.GVKey.astype(int)
        output_ignored.IdcCode = output_ignored.IdcCode.astype(int)
        output_ignored.GVKey = output_ignored.GVKey.astype(int)
        # Write to file
        output.to_csv(os.path.join(DDIR, 'temp_filtered_ids.csv'),
                      index=False)
        output_ignored.to_csv(os.path.join(
            DDIR, 'temp_filtered_ids_ignored.csv'), index=False)


def start_check_mappings():
    # Import data
    df_gvkey = import_bad_gvkeydata()
    df_idc = import_bad_idcdata()
    df_master = import_filtered_ids_master()
    df_ignored = import_filtered_ids_ignored()
    # Iterate through each IDC Code in df_idc and see if it was handled
    to_handle = []
    for code in df_idc.Code.unique():
        # Get related GVKeys
        cusips = df_idc[df_idc.Code == code].Cusip.unique()
        gvkeys = df_gvkey[df_gvkey.ShortCusip.isin(cusips)].GvKey.unique()
        # If mapped, already, make sure all gvkeys are present OR
        # missing gvkeys are in ignore table
        if code in df_master.IdcCode.unique():
            # Reference master/ignored tables
            mapped_gvkeys = np.append(
                df_master[df_master.IdcCode == code].GVKey.unique(),
                df_ignored[df_ignored.IdcCode == code].GVKey.unique())
            # Check ignored_maps table
            if len(set(gvkeys).difference(mapped_gvkeys)) > 0:
                to_handle.append(code)
                # Drop from master table
        else:
            to_handle.append(code)
    if len(to_handle):
        # Adjust master table/ignore table
        archive_file(df_master, 'filtered_ids_master')
        archive_file(df_ignored, 'filtered_ids_ignored')
        df_master[~df_master.IdcCode.isin(to_handle)].to_csv(
            os.path.join((DDIR), 'filtered_ids_master.csv'), index=False)
        df_ignored[~df_ignored.IdcCode.isin(to_handle)].to_csv(
            os.path.join((DDIR), 'filtered_ids_ignored.csv'), index=False)
        # These are the tables to handle
        df_idc[df_idc.Code.isin(to_handle)].to_csv(
            os.path.join((DDIR), 'idccodes_to_handle.csv'), index=False)


def start_commit_manually_filtered_results():
    # New Files
    output = import_temp_filtered_ids()
    output_ignored = import_temp_filtered_ids_ignored()
    # Master Files
    df_master = import_filtered_ids_master()
    df_ignored = import_filtered_ids_ignored()
    # Output new files
    df_master.append(output).to_csv(
        os.path.join((DDIR), 'filtered_ids_master.csv'), index=False)
    df_ignored.append(output_ignored).astype(int).to_csv(
        os.path.join((DDIR), 'filtered_ids_ignored.csv'), index=False)
    # Clean up to_handle table as well
    os.remove(os.path.join((DDIR), 'temp_filtered_ids.csv'))
    os.remove(os.path.join((DDIR), 'temp_filtered_ids_ignored.csv'))
    os.remove(os.path.join((DDIR), 'idccodes_to_handle.csv'))

###############################################################################

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
    # Check for ignored vals
    ignored_gvkey = gvkey_data[~gvkey_data.GvKey.isin(
        idc_obs.GVKey.unique())].GvKey.unique()
    idc_ignored = pd.DataFrame({'IdcCode': [idc] * len(ignored_gvkey),
        'GVKey': ignored_gvkey})
    return idc_obs, idc_ignored


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
                                            idc_data.EndDate.dropna()))
    return gvkey_suggestions, dates_suggestions


def _create_row(idccode, gvkey, start_date, end_date):
    tempdf = pd.DataFrame([])
    tempdf['IdcCode'] = [idccode]
    tempdf['GVKey'] = [gvkey]
    tempdf['StartDate'] = [start_date]
    tempdf['EndDate'] = [end_date]
    return tempdf


###############################################################################

def import_bad_gvkeydata():
    # Import gvkey data
    df = pd.read_csv(os.path.join(DDIR, 'bad_gvkeydata.csv'),
                     header=0, skiprows=[1])
    df.columns = [x.replace(' ', '') for x in df.columns]
    df.Changedate = convert_date_array(
        df.Changedate.apply(lambda x: x[:10]))
    df.MinReportDate = convert_date_array(
        df.MinReportDate.apply(lambda x: x[:10]))
    df.MaxReportDate = convert_date_array(
        df.MaxReportDate.apply(lambda x: x[:10]))
    df.Name = df.Name.apply(lambda x: x.rstrip())
    df.Ticker = df.Ticker.apply(lambda x: x.rstrip())
    df['ShortCusip'] = df.Cusip.apply(lambda x: x[:8])
    df = df.loc[:, ['GvKey', 'Changedate', 'MinReportDate', 'MaxReportDate',
                    'Name', 'Ticker', 'Cusip', 'ShortCusip']]
    return df


def import_bad_idcdata():
    # Import IDCData
    df = pd.read_csv(os.path.join(DDIR, 'bad_idcdata.csv'),
                      header=0, skiprows=[1])
    df.columns = [x.replace(' ', '') for x in df.columns]
    df = df.loc[:, ['Code', 'StartDate', 'EndDate', 'Cusip', 'Ticker',
                      'Issuer', 'Exchange']]
    df.StartDate = convert_date_array(df.StartDate.apply(lambda x: x[:10]))
    df.EndDate = convert_date_array(df.EndDate.apply(lambda x: x[:10]))
    df.Cusip = df.Cusip.apply(lambda x: x.rstrip())
    df.Ticker = df.Ticker.apply(lambda x: x.rstrip())
    df.Issuer = df.Issuer.apply(lambda x: x.rstrip())
    df.Exchange = df.Exchange.apply(lambda x: x.rstrip())
    df = df[df.Cusip != '']
    return df


def import_temp_filtered_ids():
    try:
        df = pd.read_csv(os.path.join(DDIR, 'temp_filtered_ids.csv'), header=0)
    except:
        df = pd.DataFrame([])
        df['IdcCode'] = []
        df['GVKey'] = []
        df['StartDate'] = []
        df['EndDate'] = []
    return df


def import_temp_filtered_ids_ignored():
    # Read master table
    try:
        df = pd.read_csv(os.path.join(DDIR, 'temp_filtered_ids_ignored.csv'))
    except:
        # TEMP UNTIL SOMETHING IS WRITTEN
        df = pd.DataFrame([])
        df['IdcCode'] = []
        df['GVKey'] = []
    return df


def import_idccodes_to_handle():
    try:
        df = pd.read_csv(os.path.join(DDIR, 'idccodes_to_handle.csv'))
    except:
        print 'Nothing to handle'
        sys.exit()
    df.StartDate = convert_date_array(df.StartDate)
    df.EndDate = convert_date_array(df.EndDate)
    return df


def import_filtered_ids_master():
    # Read master table
    df_master = pd.read_csv(os.path.join(DDIR, 'filtered_ids_master.csv'))
    return df_master


def import_filtered_ids_ignored():
    # Read master table
    df_master = pd.read_csv(os.path.join(DDIR, 'filtered_ids_ignored.csv'))
    archive_file(df_master, 'filtered_ids_ignored')
    return df_master


def archive_file(df, file_name):
    date_label = dt.datetime.utcnow().strftime('%Y%m%d')
    count = 0
    while True:
        path = os.path.join(DDIR, 'archive', '{}_{}_{}.csv'.format(
            date_label, file_name, count))
        if not os.path.exists(path):
            df.to_csv(path, index=False)
            break
        count += 1


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--check_mappings',
        action='store_true', default=False,
        help='Part of workflow to check flagged IDC Codes')
    parser.add_argument(
        '-m', '--manual_filter',
        action='store_true', default=False,
        help='To launch manual filter process to handle bad IDC Codes')
    parser.add_argument(
        '-a', '--add_new_mappings',
        action='store_true', default=False,
        help='After the manual filter, review and commit changes')
    args = parser.parse_args()

    if args.manual_filter:
        start_manual_filter()

    elif args.check_mappings:
        start_check_mappings()

    elif args.add_new_mappings:
        start_commit_manually_filtered_results()
