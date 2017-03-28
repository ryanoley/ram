import os
import sys
import json
import logging
import pandas as pd
import datetime as dt

from gearbox import ProgBar, convert_date_array

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config
from ram.utils.documentation import get_git_branch_commit
from ram.utils.documentation import prompt_for_description


class Strategy(object):

    __metaclass__ = ABCMeta

    def __init__(self, prepped_data_version, write_flag=False):
        self._write_flag = write_flag
        self.register_prepped_data_dir(prepped_data_version,
                                       config.PREPPED_DATA_DIR)
        self.register_output_dir(config.SIMULATION_OUTPUT_DIR)

    def register_prepped_data_dir(self, version, data_dir):
        self._prepped_data_version = version
        path = os.path.join(data_dir, self.__class__.__name__, version)
        self._prepped_data_dir = path

    def register_output_dir(self, data_dir):
        self._output_dir = data_dir

    # ~~~~~~ RUN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def start(self):
        self._get_data_file_names()
        self._create_output_dir()
        for i in ProgBar(range(len(self._data_files))):
            self.run_index(i)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _create_output_dir(self):
        if self._write_flag:
            # Collect some meta data
            description = prompt_for_description()
            git_branch, git_commit = get_git_branch_commit()
            # Make directory for simulations
            if not os.path.exists(self._output_dir):
                os.makedirs(self._output_dir)
            # Make directory for StrategyClass
            strategy_dir = os.path.join(self._output_dir,
                                        self.__class__.__name__)
            if not os.path.exists(strategy_dir):
                os.makedirs(strategy_dir)
            # Get all run versions
            all_dirs = [x for x in os.listdir(strategy_dir) if x[:3] == 'run']
            if all_dirs:
                new_ind = max([int(x.split('_')[1]) for x in all_dirs]) + 1
            else:
                new_ind = 1
            run_dir = os.path.join(strategy_dir, 'run_{0:04d}'.format(new_ind))
            os.mkdir(run_dir)
            # Output directory setup
            self.strategy_output_dir = os.path.join(run_dir, 'index_outputs')
            os.makedirs(self.strategy_output_dir)
            # Create meta object
            run_meta = {
                'prepped_data_version': self._prepped_data_version,
                'latest_git_commit': git_commit,
                'git_branch': git_branch,
                'description': description
            }
            with open(os.path.join(run_dir, 'meta.json'), 'w') as outfile:
                json.dump(run_meta, outfile)
            outfile.close()
            # Column parameters
            column_params = self.get_column_parameters()
            with open(os.path.join(run_dir, 'column_params.json'),
                      'w') as outfile:
                json.dump(column_params, outfile)
            outfile.close()

    def _get_data_file_names(self):
        all_files = os.listdir(self._prepped_data_dir)
        self._data_files = [x for x in all_files if x[-8:] == 'data.csv']

    # ~~~~~~ To Be Overwritten ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def run_index(self, index):
        """
        Takes in integer
        """
        raise NotImplementedError('Strategy.run_index')

    @abstractmethod
    def get_column_parameters(self):
        """
        Takes in integer
        """
        raise NotImplementedError('Strategy.get_column_parameters')

    # ~~~~~~ To Be Used by Derived Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def read_data_from_index(self, index):
        dpath = os.path.join(self._prepped_data_dir,
                             self._data_files[index])
        data = pd.read_csv(dpath)
        data.Date = convert_date_array(data.Date)
        data.SecCode = data.SecCode.astype(int).astype(str)
        return data

    def write_index_results(self, returns_df, index):
        """
        This is a wrapper function for cloud implementation.
        """
        output_name = self._data_files[index]
        output_name = output_name.replace('data', 'returns')
        if self._write_flag:
            returns_df.to_csv(os.path.join(self.strategy_output_dir,
                                           output_name))

    def write_index_stats(self, stats, index):
        output_name = self._data_files[index]
        output_name = output_name.replace('data', 'stats')
        if self._write_flag:
            with open(os.path.join(self.strategy_output_dir,
                                   output_name), 'w') as outfile:
                json.dump(stats, outfile)
            outfile.close()
