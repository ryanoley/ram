import numpy as np
import pandas as pd

from ram.utils.time_funcs import convert_date_array


def import_sql_output(path):
    data = pd.read_csv(path, header=0, skiprows=[1],
                       sep='|', skipinitialspace=True)
    # Strip white space from columns
    data.columns = [x.strip() for x in data.columns]
    # Replace all commas
    data.replace(',', '', regex=True, inplace=True)
    # Strip whitespace from columns that are still objects
    obj_cols = data.columns[np.where(data.dtypes == object)[0]].values
    # Replace remaining NULL values
    for oc in obj_cols:
        data[oc] = data[oc].str.strip()
        data[oc] = data[oc].replace('NULL', np.nan)

    # Convert date columns
    def _strip_date(date):
        try:
            return date[:10]
        except:
            return date

    date_cols = [x for x in data.columns if x.find('Date') >= 0]
    for dc in date_cols:
        data[dc] = data[dc].apply(_strip_date)
        data[dc] = convert_date_array(data[dc])
    return data
