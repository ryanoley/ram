import pandas as pd

from ram.utils.time_funcs import convert_date_array


def import_sql_output(path):
    data = pd.read_csv(path, header=0, skiprows=[1],
                       delimiter=' *, *', engine='python',
                       na_values=['NULL'])
    # Strip and convert date columns
    date_cols = [x for x in data.columns if x.find('Date') >= 0]
    for dc in date_cols:
        data[dc] = data[dc].apply(lambda x: x[:10])
        data[dc] = convert_date_array(data[dc])
    return data
