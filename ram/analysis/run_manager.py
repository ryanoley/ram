import os
import json
import numpy as np
import pandas as pd

from gearbox import convert_date_array

from ram import config


class RunManager(object):

    def __init__(self, strategy_class, run_name, start_year=1950):
        self.strategy_class = strategy_class
        self.run_name = run_name
        self.start_year = start_year

    # ~~~~~~ Viewing Available Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @staticmethod
    def get_strategies(path=config.SIMULATION_OUTPUT_DIR):
        dirs = [x for x in os.listdir(path) if x.find('Strat') >= 0]
        return dirs

    @staticmethod
    def get_run_names(strategy_class, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, strategy_class)
        dirs = [x for x in os.listdir(ddir) if x.find('run') >= 0]
        output = pd.DataFrame({'Run': dirs, 'Description': np.nan})
        for i, d in enumerate(dirs):
            desc = json.load(open(os.path.join(ddir, d, 'meta.json'), 'r'))
            output.loc[i, 'Description'] = desc['description']
        return output[['Run', 'Description']]

    # ~~~~~~ Import Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def import_return_frame(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('returns') > 0]
        returns = pd.DataFrame()
        for f in files:
            if int(f[:4]) < self.start_year:
                continue
            returns = returns.add(
                pd.read_csv(os.path.join(ddir, f), index_col=0),
                fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.returns = returns

    def import_stats(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('stats') > 0]
        self.stats = {}
        if files:
            for f in files:
                self.stats[f] = json.load(open(os.path.join(ddir, f), 'r'))
        else:
            self.stats['20100101NOSTATS'] = {x: {'no_stat': -999} for x in self.column_params}

    def import_column_params(self, path=config.SIMULATION_OUTPUT_DIR):
        ppath = os.path.join(path, self.strategy_class, self.run_name,
                             'column_params.json')
        self.column_params = json.load(open(ppath, 'r'))

    def import_meta(self, path=config.SIMULATION_OUTPUT_DIR):
        ppath = os.path.join(path, self.strategy_class, self.run_name,
                             'meta.json')
        self.meta = json.load(open(ppath, 'r'))

    # ~~~~~~ Analysis Functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def analyze_parameters(self):
        if not hasattr(self, 'returns'):
            self.import_return_frame()
        if not hasattr(self, 'column_params'):
            self.import_column_params()
        if not hasattr(self, 'stats'):
            self.import_stats()
        cparams = classify_params(self.column_params)
        astats = aggregate_statistics(self.stats, self.start_year)
        return format_param_results(self.returns, cparams,
                                    astats, self.start_year)


###############################################################################

def aggregate_statistics(stats, start_year):
    # Stats are organized by dates first so need to flip to aggregate
    # for each column
    agg_stats = {k: {k: [] for k in stats.values()[0].values()[0].keys()}
                 for k in stats.values()[0].keys()}

    for t_index in stats.keys():
        if int(t_index[:4]) < start_year:
            continue
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


def format_param_results(data, cparams, astats, start_year):
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
