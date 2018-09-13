import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools as it
import matplotlib.pyplot as plt

from ram.strategy.base import Strategy


class BirdsStrategy(Strategy):

    def strategy_init(self):
        pass

    def get_data_blueprint_container(self):
        pass

    def get_strategy_source_versions(self):
        pass

    def process_raw_data(self, data, time_index, market_data=None):
        pass

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        pass

    def get_implementation_param_path(self):
        pass

    def process_implementation_params(self):
        pass
