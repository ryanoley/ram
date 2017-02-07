"""
Analysis tools for the parameters that are supplied by a strategy.
"""
import json
import pandas as pd

from gearbox import read_csv, convert_date_array


def analyze_parameters(data_path, param_path):
    df = read_csv(data_path)
    df.Date = convert_date_array(df.Date)
    df = df.set_index('Date')
    with open(param_path) as json_data:
        params = json.load(json_data)
    json_data.close()
    # Analysis
    cparams = classify_params(params)
    return format_param_results(df, cparams)


def classify_params(params):
    out = {}
    for i, p in params.iteritems():
        for k, v in p.iteritems():
            if k not in out:
                out[k] = {}
            if v not in out[k]:
                out[k][v] = []
            out[k][v].append(i)
            out[k][v].sort()
    return out


def format_param_results(data, cparams):
    out = []
    for k, p in cparams.iteritems():
        for v, cols in p.iteritems():
            s1 = data.loc[:, cols].sum().mean()
            s2 = (data.loc[:, cols].mean() / data.loc[:, cols].std()).mean()
            out.append([k, v, len(cols), s1, s2])
    out = pd.DataFrame(out, columns=['Param', 'Val', 'Count',
                                     'MeanTotalRet', 'MeanSharpe'])
    out = out.sort_values(['Param', 'Val']).reset_index(drop=True)
    return out


if __name__ == '__main__':
    import os
    # Import data
    ddir = '/Users/mitchellsuter/Desktop/statarb'
    data_path = os.path.join(ddir, 'statarb_run.csv')
    param_path = os.path.join(ddir, 'stat_arb_params.json')
    analyze_parameters(data_path, param_path)
