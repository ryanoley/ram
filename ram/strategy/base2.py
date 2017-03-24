import os
import json
import logging
import pandas as pd
import datetime as dt

from gearbox import ProgBar

from abc import ABCMeta, abstractmethod, abstractproperty

from ram import config

PREPPED_DATA_DIR = config.PREPPED_DATA_DIR
SIMULATION_OUTPUT_DIR = config.SIMULATION_OUTPUT_DIR


class Strategy2(object):

    __metaclass__ = ABCMeta

    def __init__(self, prepped_data_version, write_flag=False):
        self._write_flag = write_flag
        self.register_prepped_data_dir(prepped_data_version, PREPPED_DATA_DIR)
        self.register_output_dir(SIMULATION_OUTPUT_DIR)

    def register_prepped_data_dir(self, version, data_dir):
        try:
            self._prepped_data_version = version
            path = os.path.join(data_dir, self.__class__.__name__, version)
            all_files = os.listdir(path)
            self._data_files = [x for x in all_files if x[-8:] == 'data.csv']
            self._prepped_data_dir = path
        except:
            logging.warn('Strategy: No prepped data connected to instance.')
            self._data_files = []

    def register_output_dir(self, data_dir):
        self._output_dir = data_dir

    # ~~~~~~ RUN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def start(self):
        self._create_output_dir()
        # Collect some meta data
        description = self._prompt_for_description()
        git_branch, git_commit = self._get_git_branch_commit()
        start_time = str(dt.datetime.utcnow())

        for i in ProgBar(range(len(self._data_files))):
            self.run_index(i)

        run_meta = {
            'prepped_data_version': self._prepped_data_version,
            'latest_git_commit': git_commit,
            'git_branch': git_branch,
            'run_start_time': start_time,
            'run_end_time': str(dt.datetime.utcnow()),
            'description': self._description
        }

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _prompt_for_description(self):
        if self._write_flag:
            desc = raw_input("\nEnter a description of this run:\n")
            if len(desc) == 0:
                print '\nMust enter description!!\n'
                desc = self._prompt_for_description()
            return desc
        else:
            return None

    def _get_git_branch_commit(self):
        """
        This is used for documenting where the simulation came from.
        """
        repo_dir = os.path.join(os.getenv('GITHUB'), 'ram')
        git_branch = open(os.path.join(repo_dir, '.git/HEAD'), 'r').read()
        git_branch = git_branch.split('/')[-1].replace('\n', '')
        git_commit = os.path.join(repo_dir, '.git/refs/heads', git_branch)
        git_commit = open(git_commit).read().replace('\n', '')
        return git_branch, git_commit

    def _create_output_dir(self):
        if self._write_flag:
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
            # Output directory setup
            self.strategy_output_dir = os.path.join(
                strategy_dir, 'run_{0:04d}'.format(new_ind))
            os.makedirs(self.strategy_output_dir)

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
        return pd.read_csv(dpath)

    def write_index_output(self, index):
        """
        This is a wrapper function for cloud implementation.
        """
        pass
