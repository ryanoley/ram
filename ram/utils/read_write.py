import pandas as pd

from ram.utils.time_funcs import convert_date_array


def import_sql_output(path):
    data = pd.read_csv(path, header=0, skiprows=[1],
                       sep='|', engine='python',
                       na_values=['NULL'])
    # Strip white space from columns
    data.columns = [x.strip() for x in data.columns]
    # Replace all commas
    data.replace(',', '', regex=True, inplace=True)
    # Convert date columns
    date_cols = [x for x in data.columns if x.find('Date') >= 0]
    for dc in date_cols:
        data[dc] = data[dc].apply(lambda x: x[:10])
        data[dc] = convert_date_array(data[dc])
    return data
