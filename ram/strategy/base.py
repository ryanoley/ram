import os
import json
import shutil
import subprocess
import pandas as pd
import datetime as dt

from abc import ABCMeta, abstractmethod, abstractproperty

from ram.data.dh_sql import DataHandlerSQL
from gearbox import ProgBar


OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'simulations')


class Strategy(object):

    __metaclass__ = ABCMeta

    def __init__(self, write_flag=False):
        self._set_output_dir(write_flag)
        # Connect to QADirect
        self.datahandler = DataHandlerSQL()

    def start(self):
        """
        This should be the method implemented when running a strategy.
        """
        try:
            # Meta data
            start_time = str(dt.datetime.utcnow())
            git_branch, git_commit = self._get_git_branch_commit()

            # Objects that can be returned by run_index
            returns = pd.DataFrame()
            column_params = {}
            statistics = {}

            for i in ProgBar(self.get_iter_index()):
                output = self.run_index(i)
                if isinstance(output, dict):
                    rets = output['returns']
                elif isinstance(output, pd.DataFrame):
                    rets = output
                elif output is None:
                    # For DEBUGGING
                    continue
                # Enforce that the index is DateTime
                assert isinstance(rets.index, pd.DatetimeIndex)
                returns = returns.add(rets, fill_value=0)
                if 'statistics' in output:
                    statistics[i] = output['statistics']

            self.returns = returns
            if 'column_params' in output:
                self.column_params = output['column_params']
            else:
                self.column_params = None
            if len(statistics) > 0:
                self.statistics = statistics
            else:
                self.statistics = None
            self.run_meta = {
                'latest_git_commit': git_commit,
                'git_branch': git_branch,
                'run_start_time': start_time,
                'run_end_time': str(dt.datetime.utcnow())
            }
            self._write_results()

        except KeyboardInterrupt:
            print '\nClosing Database Connection'
            self.datahandler.close_connections()
            raise KeyboardInterrupt

        self.datahandler.close_connections()

    def run_index_writer(self, index):
        """
        This is a wrapper function for cloud implementation.
        """
        results = self.run_index(index)
        if isinstance(results, dict):
            results = results['returns']
        # Enforce that the index is DateTime
        assert isinstance(results.index, pd.DatetimeIndex)
        results.to_csv(self.strategy_output_dir+'/result_{0:05d}.csv'.format(index))

    def _prompt_for_description(self):
        desc = raw_input("\nEnter a description of this run:\n")
        if len(desc) == 0:
            print '\nMust enter description!!\n'
            desc = self._prompt_for_description()
        return desc

    def _set_output_dir(self, write_flag):
        self.write_flag = write_flag
        if write_flag:
            self._description = self._prompt_for_description()
            # Make directory for simulations
            if not os.path.exists(OUTDIR):
                os.makedirs(OUTDIR)
            # Make directory for StrategyClass
            strategy_dir = os.path.join(OUTDIR, self.__class__.__name__)
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

    def _write_results(self):
        if self.write_flag:
            self.returns.to_csv(os.path.join(self.strategy_output_dir,
                                             'results.csv'))
            if self.column_params:
                with open(os.path.join(self.strategy_output_dir,
                                       'params.json'), 'w') as outfile:
                    json.dump(self.column_params, outfile)
                outfile.close()
            if self.statistics:
                with open(os.path.join(self.strategy_output_dir,
                                       'statistics.json'), 'w') as outfile:
                    json.dump(self.statistics, outfile)
                outfile.close()
            if self._description:
                pass
            run_meta['description'] = self._description
            with open(os.path.join(self.strategy_output_dir,
                                   'meta.json'), 'w') as outfile:
                json.dump(self.run_meta, outfile)
            outfile.close()

    def _get_git_branch_commit(self):
        repo_dir = os.path.join(os.getenv('GITHUB'), 'ram')

        git_branch = open(os.path.join(repo_dir, '.git/HEAD'), 'r').read()
        git_branch = git_branch.split('/')[-1].replace('\n', '')

        git_commit = os.path.join(repo_dir, '.git/refs/heads', git_branch)
        git_commit = open(git_commit).read().replace('\n', '')

        return git_branch, git_commit

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def run_index(self):
        """
        Takes in integer
        """
        raise NotImplementedError('Strategy.run_index')

    @abstractmethod
    def get_iter_index(self):
        """
        Returns list of integers that will be run by run_index
        """
        raise NotImplementedError('Strategy.get_iter_index')
