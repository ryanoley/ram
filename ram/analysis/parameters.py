"""
Analysis tools for the parameters that are supplied by a strategy.
"""
import numpy as np
import pandas as pd


def aggregate_statistics(stats):
    # Stats are organized by dates first so need to flip to aggregate
    # for each column
    agg_stats = {k: {k: [] for k in stats.values()[0].values()[0].keys()}
                 for k in stats.values()[0].keys()}

    for t_index in stats.keys():
        for col, col_stats in stats[t_index].iteritems():
            for key, val in col_stats.iteritems():
                agg_stats[col][key].append(val)

    # Aggregate column stats overtime
    out_stats = {}
    for col, col_stats in agg_stats.iteritems():
        out_stats[col] = {}
        for key, vals in col_stats.iteritems():
            out_stats[col][key] = (np.mean(vals), np.std(vals))

    return out_stats


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


def format_param_results(data, cparams, astats):
    out = []
    stat_names = astats.values()[0].keys()
    stat_names.sort()
    for k, p in cparams.iteritems():
        for v, cols in p.iteritems():
            s1 = data.loc[:, cols].sum().mean()
            s2 = (data.loc[:, cols].mean() / data.loc[:, cols].std()).mean()

            col_stats = [astats[c] for c in cols]
            # Agg by stat
            agg_stats = {k: [] for k in stat_names}
            for stat in col_stats:
                for key, val in stat.iteritems():
                    agg_stats[key].append(val[0])
            # Average each stat and add to data
            st_list = []
            for stat in stat_names:
                st_list.append(np.mean(agg_stats[stat]))
            out.append([k, v, len(cols), s1, s2] + st_list)

    out = pd.DataFrame(out, columns=['Param', 'Val', 'Count',
                                     'MeanTotalRet', 'MeanSharpe'] +
                       stat_names)
    out = out.sort_values(['Param', 'Val']).reset_index(drop=True)
    return out
