import os
import pickle
import pandas as pd

from ram import config
from ram.strategy.statarb import statarb_config


def import_raw_data():
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'daily_raw_data')

    all_files = os.listdir(path)
    max_date = max([x.split('_')[0] for x in all_files])
    todays_files = [x for x in all_files if x.find(max_date) > -1]
    todays_files = [x for x in todays_files if x.find('.csv') > -1]

    data = {}
    for f in todays_files:
        name = f[f.rfind('version'):].replace('.csv', '')
        data[name] = pd.read_csv(os.path.join(path, f))


