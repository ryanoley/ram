import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping')
DDIR = '/Users/mitchellsuter/Desktop/gvkey_mapping'


# Confirm filtered data Start and End Dates are correct
df_filtered = pd.read_csv(os.path.join(DDIR, 'filtered_ids.csv'))
df_filtered.StartDate = convert_date_array(df_filtered.StartDate)
df_filtered.EndDate = convert_date_array(df_filtered.EndDate)


for code in df_filtered.IdcCode.unique():
    df = df_filtered[df_filtered.IdcCode == code]
    if np.any(df.StartDate - df.EndDate.shift(1) > dt.timedelta(days=1)):
        print df

