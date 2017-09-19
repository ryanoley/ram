
import os
import shutil
from tqdm import tqdm
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.data.extract_alpha import read_all_extract_alpha_files
from ram.strategy.long_pead.data.data_container1 import DataContainer1
from ram.strategy.long_pead.data.extract_alpha import get_extract_alpha_data
from ram.strategy.long_pead.utils import outlier_rank

from gearbox import convert_date_array


def _process_ea(data, features):
    data2 = outlier_rank(data, features[0])
    for f in features[1:]:
        data2 = data2.merge(outlier_rank(data, f))
    # Drop extremes
    drop_cols = [x for x in data2.columns if x.find('extreme') > 0]
    data2 = data2.drop(drop_cols, axis=1).fillna(0)
    return data2


def _merge_extract_alpha(data):
    min_date = data.Date.min()
    max_date = data.Date.max()

    v1 = _extract_alpha_data['tress']
    tdata = v1[0]
    tdata = tdata[(tdata.Date >= min_date)&(tdata.Date <= max_date)]
    ea_data = _process_ea(
        get_extract_alpha_data(tdata, v1[1], data), v1[1])
    data = data.merge(ea_data, how='left')
    ea_features = list(v1[1])

    v1 = _extract_alpha_data['cam1']
    tdata = v1[0]
    tdata = tdata[(tdata.Date >= min_date)&(tdata.Date <= max_date)]
    ea_data = _process_ea(
        get_extract_alpha_data(tdata, v1[1], data), v1[1])
    data = data.merge(ea_data, how='left')
    ea_features.append(v1[1])

    v1 = _extract_alpha_data['tm']
    tdata = v1[0]
    tdata = tdata[(tdata.Date >= min_date)&(tdata.Date <= max_date)]
    ea_data = _process_ea(
        get_extract_alpha_data(tdata, v1[1], data), v1[1])
    data = data.merge(ea_data, how='left')
    ea_features.append(v1[1])

    v1 = _extract_alpha_data['dr']
    tdata = v1[0]
    tdata = tdata[(tdata.Date >= min_date)&(tdata.Date <= max_date)]
    ea_data = _process_ea(
        get_extract_alpha_data(tdata, v1[1], data), v1[1])
    data = data.merge(ea_data, how='left')
    ea_features.append(v1[1])

    # HACK to rename columns
    ea_features2 = []
    for x in ea_features:
        if isinstance(x, list):
            ea_features2.extend(x)
        else:
            ea_features2.append(x)
    ea_features3 = ['EA_' + x for x in ea_features2]

    all_columns = data.columns.values
    for i, col in enumerate(all_columns):
        ind = np.where(col == np.array(ea_features2))[0]
        if len(ind) > 0:
            all_columns[i] = ea_features3[ind[0]]
    data.columns = all_columns

    return data




_extract_alpha_data = read_all_extract_alpha_files()


ddir = os.path.join(os.getenv('DATA'), 'ram',
                    'prepped_data', 'LongPeadStrategy')

old_ddir = os.path.join(ddir, 'version_0108')
new_ddir = os.path.join(ddir, 'version_0999')


# if os.path.isdir(new_ddir):
#     shutil.rmtree(new_ddir)
# 
# os.mkdir(new_ddir)

# shutil.copyfile(os.path.join(old_ddir, 'meta.json'), os.path.join(new_ddir, 'meta.json'))
# shutil.copyfile(os.path.join(old_ddir, 'market_index_data.csv'), os.path.join(new_ddir, 'market_index_data.csv'))

all_files = os.listdir(old_ddir)
all_files = [x for x in all_files if x.find('market') == -1]
all_files = [x for x in all_files if x.find('meta') == -1]


for fn in tqdm(all_files):

    if os.path.isfile(os.path.join(new_ddir, fn)):
        continue

    df = pd.read_csv(os.path.join(old_ddir, fn))
    df.Date = convert_date_array(df.Date)
    df.SecCode = df.SecCode.astype(int).astype(str)

    min_date = df.Date[df.TestFlag].min()
    trim_date = min_date - dt.timedelta(days=80)
    trim_date = dt.date(trim_date.year, trim_date.month, 1)
    df = df[df.Date >= trim_date].copy()

    df, _ = DataContainer1()._process_data(df)
    df = _merge_extract_alpha(df)

    df.to_csv(os.path.join(new_ddir, fn))




