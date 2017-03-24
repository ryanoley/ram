import os
import pandas as pd

from gearbox import read_csv


def get_extreme_spreads_rets(data, var):
    qts = _get_var_spreads(data, var)
    df2 = data.merge(qts)
    df2['TopFlag'] = df2[var] >= df2.Top
    df2['BottomFlag'] = df2[var] <= df2.Bottom
    out = df2[df2.TopFlag].groupby('Date')['Ret'].mean().reset_index()
    out.columns = ['Date', 'TopRet']
    out['BottomRet'] =  df2[df2.BottomFlag].groupby(
        'Date')['Ret'].mean().values
    var1 = '{}_RetSpread'.format(var)
    var2 = '{}_VarSpread'.format(var)
    out[var1] = out.TopRet - out.BottomRet
    out[var2] = qts.Spread
    return out[['Date', var1, var2]]


def _get_var_spreads(data, var):
    out = data.groupby('Date')[var].quantile(.05).reset_index()
    out.columns = ['Date', 'Bottom']
    out['Top'] = data.groupby('Date')[var].quantile(.95).values
    out['Spread'] = out.Top - out.Bottom
    return out


def import_data_file(path):
    # Import, create indexes, and append to final
    df = read_csv(path)
    df['Ret'] = df.LEAD1_AdjClose / df.AdjClose - 1
    df = df.drop(['AdjClose', 'LEAD1_AdjClose', 'LEAD2_AdjClose',
                  'LEAD3_AdjClose', 'SI', 'TestFlag'], axis=1)
    variables = df.columns.tolist()
    variables = [x for x in variables if x not in ['SecCode', 'Date', 'Ret']]
    return df.dropna(), variables


ddir = os.path.join(os.getenv('DATA'), 'ram', 'prepped_data', 'BirdsStrategy', 'version_001')

files = os.listdir(ddir)

for f in files:

    df, variables = import_data_file(os.path.join(ddir, f))

    for var in variables:
        out = get_extreme_spreads_rets(df, var)
        if var == variables[0]:
            var_out = out
        else:
            var_out = var_out.merge(out)

    if f == files[0]:
        master_out = var_out
    else:
        master_out = master_out.append(var_out)

    print f

master_out = master_out.set_index('Date')

outdir = os.path.join(os.getenv('DATA'), 'ram', 'prepped_data', 'BirdsStrategy')

master_out.to_csv(os.path.join(outdir, 'master_features.csv'))

