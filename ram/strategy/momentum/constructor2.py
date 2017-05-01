import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.momentum.portfolio2 import Portfolio2


class MomentumConstructor2(object):

    def __init__(self):
        self.portfolios = {}

    def get_iterable_args(self):
        return {'holding_period': [20, 25, 30],
                'positions_per_side': [10, 30, 50],
                'n_ports': [2, 4, 6, 8],
                'train_period_len': [100, 240]}

    def get_daily_returns(self, data, frequency, arg_index,
                          holding_period, positions_per_side,
                          n_ports, train_period_len):
        """
        Parameters
        ----------
        data : pd.DataFrame
        frequency : str
            'Q' or 'M' for quarterly or monthly
        arg_index : int
            Used to separate portfolios over different parameter
            settings.
        """
        # Can't have more positions than days you can open a portfolio
        assert n_ports <= holding_period
        # Add portfolio if not in arg index
        if arg_index not in self.portfolios:
            self.portfolios[arg_index] = Portfolio2(n_ports)
        portfolio = self.portfolios[arg_index]

        date_iterable = self._get_date_iterable(
            data, frequency, holding_period, n_ports)

        m_signals, m_prices = self._get_signals(data, train_period_len,
                                                positions_per_side)

        output = pd.DataFrame({'Ret': 0}, index=date_iterable[0])

        for date, entry_flag, exit_flag in zip(*zip(*date_iterable)):
            portfolio.update_prices(m_prices[date])
            if exit_flag:
                # Close first because this will then be replaced
                portfolio.close_portfolio(entry_flag)
            if entry_flag:
                portfolio.add_positions(entry_flag, m_signals[date])
            ret = portfolio.get_daily_return()

        return output, {}

    # ~~~~~~ DATA ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_signals(self, data, train_period_len, positions_per_side):
        # This dataframe is used for trading prices but also to construct
        # some factors
        m_prices = data.pivot(index='Date',
                              columns='SecCode',
                              values='AdjClose')
        returns = m_prices.pct_change()
        # Get dates
        all_dates = data.Date.unique()
        test_period_dates = data[data.TestFlag].Date.unique()

        # CRITERIA: Beta, big moves, volatility, volume
        volatility = returns.rolling(train_period_len).std()
        # Period return
        period_return = returns.rolling(train_period_len).sum()
        # Count per group for the first sort
        bottom_rank = np.ceil(period_return.shape[1] * .25)
        top_rank = period_return.shape[1] - bottom_rank + 1
        sort_one_ranks = period_return.rank(axis=1)
        # Top/Bottom 25%
        inds = sort_one_ranks >= top_rank
        sort_two_ranks_tops = volatility[inds].rank(axis=1)
        inds = sort_one_ranks <= bottom_rank
        sort_two_ranks_bottoms = volatility[inds].rank(axis=1)
        # Flags
        long_flags = sort_two_ranks_tops <= positions_per_side
        short_flags = sort_two_ranks_bottoms >= \
            (bottom_rank - positions_per_side + 1)
        m_factor = long_flags.astype(int) - short_flags.astype(int)
        # Convert to dictionaries
        return m_factor.loc[test_period_dates].T.to_dict(), \
            m_prices.loc[test_period_dates].T.to_dict()

    def _get_date_iterable(self, data, freq, holding_period, n_ports):
        train_period_len = len(data[~data.TestFlag].Date.unique())
        # Get dates for THIS period.
        # iterable_dates will extend beyond these dates
        all_dates = data[data.TestFlag].Date.unique()
        if freq == 'Q':
            periods = np.array([(x.month-1)/3+1 for x in all_dates])
        elif freq == 'M':
            periods = np.array([x.month for x in all_dates])
        else:
            raise Exception('Frequency of universe re-creation not given')
        period_dates = all_dates[periods == periods[0]]
        # Get offset
        offset = holding_period / n_ports
        # Carve them up
        base_index_entry = np.arange(0, len(period_dates), holding_period)
        base_index_exit = np.append(base_index_entry[1:],
                                    base_index_entry[-1]+holding_period)
        # Add additional for close flag
        entry_flags = np.zeros(len(all_dates))
        exit_flags = np.zeros(len(all_dates))
        for i in range(n_ports):
            entry_flags[base_index_entry + (offset * i)] = (i+1)
            exit_flags[base_index_exit + (offset * i)] = (i+1)
        cut_index = np.max(np.where(exit_flags != 0)[0])+1
        return zip(all_dates[:cut_index],
                   entry_flags[:cut_index],
                   exit_flags[:cut_index])
