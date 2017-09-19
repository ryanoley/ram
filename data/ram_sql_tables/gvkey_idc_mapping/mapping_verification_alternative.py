import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.utils.time_funcs import convert_date_array
from ram.utils.read_write import import_sql_output

DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping')

from ram.utils.gvkey_mapping import *



df = import_sql_output(os.path.join(DDIR, 'bad_gvkey_mapping.txt'))

#df = import_sql_output(os.path.join(DDIR, 'good_gvkey_mapping.txt'))
#df['StartDate'] = dt.datetime(1959, 1, 1)
#df['EndDate'] = dt.datetime(2079, 1, 1)

### MAP IdcCodes to GVKeys


handled_ids = pd.DataFrame(columns=['IdcCode', 'GVKey', 'StartDate', 'EndDate'])

while len(df):

    ic = df.Code.iloc[0]

    gvkeys = df.GVKey[df.Code == ic].unique()
    idccodes = df.Code[df.GVKey.isin(gvkeys)].unique()

    data = df[df.GVKey.isin(gvkeys) & df.Code.isin(idccodes)].copy()

    # SIMPLE ROLL UP because this table only goes IdcCode -> GVKey
    if len(gvkeys) == 1:
        # Basic IdcCode change. Must find correct start date
        result = one_gvkey_rollup(data)
        handled_ids = handled_ids.append(result)

    elif (len(idccodes) == 1) & (len(gvkeys) == 2):
        result = two_gvkeys_one_idccode(data)
        handled_ids = handled_ids.append(result)

    elif (len(idccodes) == 1) & (len(gvkeys) > 2):
        print(len(gvkeys))

    else:
        print(len(idccodes), len(gvkeys))
        result = manually_handle(data)
        handled_ids = handled_ids.append(result)

    df = df[~df.GVKey.isin(gvkeys) & ~df.Code.isin(idccodes)]




def manually_handle(data):
    # Iterate unique IdcCodes
    import pdb; pdb.set_trace()
    for ic in data.Code.unique():
        temp = data[data.Code == ic]
        min_date = temp.IdcCodeStartDate.min()
        max_date = temp.IdcCodeEndDate.max()
        temp = temp[
            ['Code', 'GVKey', 'GVKeyChangeDate',
             'MinReportDate', 'MaxReportDate']].drop_duplicates()
        temp = temp.sort_values('GVKeyChangeDate')
        print 'Min IdcDate: %s' % min_date
        print 'Max IdcDate: %s' % max_date
        print(temp)

    X = 10
    return None



