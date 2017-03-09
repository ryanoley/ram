import os
import json
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

from gearbox import read_csv, convert_date_array

from ram.analysis.parameters import *
from ram.analysis.statistics import get_stats


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_run_directories(search_dir):
    out = []
    for path, _, file_names in os.walk(search_dir):
        if os.path.basename(path)[:3] == 'run' and 'results.csv' in file_names:
            out.append(path)
    return out


def analyze_run(run_path, start_year=1993):
    df = read_csv(os.path.join(run_path, 'results.csv'))
    df.index = convert_date_array(df.index)
    with open(os.path.join(run_path, 'params.json')) as json_data:
        params = json.load(json_data)
    json_data.close()
    # Get meta data
    with open(os.path.join(run_path, 'meta.json')) as json_data:
        meta = json.load(json_data)
    json_data.close()
    # Get stats
    with open(os.path.join(run_path, 'statistics.json')) as json_data:
        stats = json.load(json_data)
    json_data.close()
    # See if description exists in meta data to print to screen
    if 'description' in meta:
        description = meta['description']
    else:
        description = 'No description'
    # Analysis
    cparams = classify_params(params)
    astats = aggregate_statistics(stats)
    # Format and output
    print description
    print format_param_results(df, cparams, astats)
    plt.figure()
    plt.plot(df.cumsum())
    plt.grid()
    plt.show()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_combo_directories(search_dir):
    out = []
    for path, _, file_names in os.walk(search_dir):
        if os.path.basename(path)[:5] == 'combo' and \
        'best_returns.csv' in file_names:
            out.append(path)
    return out


def analyze_combo(combo_path, start_year=1993):
    df = read_csv(os.path.join(combo_path, 'best_returns.csv')).dropna()
    df.index = convert_date_array(df.index)
    inds = np.array([x.year for x in df.index]) >= start_year
    df.index = pd.DatetimeIndex(df.index)
    df = df.iloc[inds]
    print get_stats(df)
    plt.figure()
    plt.plot(df.cumsum())
    plt.grid()
    plt.show()


def compare_combos(combo_path1, combo_path2, start_year=1993):
    def _import_data(combo_path):
        df = read_csv(os.path.join(combo_path, 'best_returns.csv')).dropna()
        df.index = convert_date_array(df.index)
        inds = np.array([x.year for x in df.index]) >= start_year
        df.index = pd.DatetimeIndex(df.index)
        df = df.iloc[inds]
    df1 = _import_data(combo_path1)
    df2 = _import_data(combo_path2)
    plt.figure()
    plt.plot(df1.cumsum(), label='combo1', color='b')
    plt.plot(df2.cumsum(), label='combo2', color='g')
    plt.legend(loc=2)
    plt.grid()
    plt.show()
