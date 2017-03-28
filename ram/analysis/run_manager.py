import os
import json
import pandas as pd

from gearbox import convert_date_array

from ram import config


class RunManager(object):

    def __init__(self, strategy_class, run_name):
        self.strategy_class = strategy_class
        self.run_name = run_name

    def create_return_frame(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('returns') > 0]
        returns = pd.DataFrame()
        for f in files:
            returns = returns.add(
                pd.read_csv(os.path.join(ddir, f), index_col=0),
                fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.returns = returns
        return

    def import_stats(self, path=config.SIMULATION_OUTPUT_DIR):
        ddir = os.path.join(path, self.strategy_class, self.run_name,
                            'index_outputs')
        files = [x for x in os.listdir(ddir) if x.find('stats') > 0]
        self.stats = {}
        for f in files:
            self.stats[f] = json.load(open(os.path.join(ddir, f), 'r'))
        return

    def import_column_params(self, path=config.SIMULATION_OUTPUT_DIR):
        ppath = os.path.join(path, self.strategy_class, self.run_name,
                             'column_params.json')
        self.column_params = json.load(open(ppath, 'r'))
        return

    def import_meta(self, path=config.SIMULATION_OUTPUT_DIR):
        ppath = os.path.join(path, self.strategy_class, self.run_name,
                             'meta.json')
        self.meta = json.load(open(ppath, 'r'))
        return
