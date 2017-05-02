import logging
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.constructor.portfolio import PairPortfolio


class PortfolioConstructor2(object):

    def __init__(self, position_size=100000):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.position_size = position_size
        self.booksize = 100e6
        self._portfolios = {}
        self._data = {}
        self._triggers = {}

    def get_iterable_args(self):
        return {
            'max_pos_count': [6, 15, 30],
            'pos_perc_deviation': [0.10],
            'z_entry': [2.3, 2.5, 2.8],
            'z_exit': [0, 0.25, 0.5],
            'remove_earnings': [False, True],
            'max_holding_days': [10, 30]
        }

    def get_daily_pl(self,
                     arg_index,
                     max_pos_count,
                     pos_perc_deviation,
                     z_entry,
                     z_exit,
                     remove_earnings,
                     max_holding_days):

        if arg_index not in self._portfolios:
            self._portfolios[arg_index] = PairPortfolio()
            self._triggers[arg_index] = {}

        portfolio = self._portfolios[arg_index]
        triggers = self._triggers[arg_index]

        # Output object
        daily_df = pd.DataFrame(index=self.iter_dates,
                                columns=['PL', 'Exposure'],
                                dtype=float)

        for date in self.iter_dates:

            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            ern_flags = self.earnings_flags[date]
            z_scores = self.z_scores[date]
            new_z_scores = self.new_z_scores[date]

            # 1. Update all the prices in portfolio. This calculates PL
            #    for individual positions
            portfolio.update_prices(closes, dividends, splits)

            # 2. CLOSE PAIRS
            #  Closed pairs are still in portfolio dictionary
            #  and must be cleaned at end
            self._close_signals(portfolio, z_scores, z_exit,
                                ern_flags, remove_earnings,
                                max_holding_days)

            # 3. ADJUST POSITIONS
            #  When the exposures move drastically (say when the markets)
            #  go up or down, it affects the size of the new positions
            #  quite significantly
            self._adjust_open_positions(portfolio, pos_perc_deviation)

            # 4. OPEN NEW PAIRS
            self._execute_open_signals(portfolio, triggers,
                                       new_z_scores, z_scores, closes,
                                       max_pos_count, z_entry,
                                       ern_flags, remove_earnings)

            # Report PL and Exposure
            daily_df.loc[date, 'PL'] = portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = self.booksize

        return daily_df, portfolio.get_period_stats()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _close_signals(self, portfolio, scores, z_exit, ern_flags,
                       remove_earnings, max_holding_days):
        close_pairs = []
        # Get current position z-scores, and decide if they need to be closed
        for pair in portfolio.pairs.keys():
            if np.abs(scores[pair]) < z_exit or np.isnan(scores[pair]):
                close_pairs.append(pair)
            # EARNINGS
            p1, p2 = pair.split('~')
            if (ern_flags[p1] or ern_flags[p2]) and remove_earnings:
                close_pairs.append(pair)
            if portfolio.pairs[pair].stat_holding_days >= \
                    max_holding_days:
                close_pairs.append(pair)
        # Close positions
        portfolio.close_pairs(list(set(close_pairs)))
        return

    def _adjust_open_positions(self, portfolio,
                               pos_perc_deviation=0.03):
        portfolio.update_position_exposures(self.position_size,
                                            pos_perc_deviation)

    def _execute_open_signals(self,
                              portfolio,
                              triggers,
                              new_z_scores,
                              z_scores,
                              trade_prices,
                              max_pos_count,
                              z_entry,
                              ern_flags,
                              remove_earnings):
        """
        Function that adds new positions.
        """
        open_pairs = portfolio.get_open_positions()
        closed_pairs = portfolio.get_closed_positions()
        # Drop open and closing pairs from scores
        no_go_pairs = sum([open_pairs, closed_pairs], [])

        # Iterate to see if any new triggers
        for z, side, pair in new_z_scores:
            if (z > z_entry) and (pair not in triggers) and (pair not in no_go_pairs):
                triggers[pair] = side

        # Iterate to see if any new positions need to be put on
        new_pos = []
        for pair, side in triggers.iteritems():
            if abs(z_scores[pair]) < z_entry:
                new_pos.append((pair, side))

        # Remove from dictionary new paris
        for p in new_pos:
            triggers.pop(p[0])

        self._get_pos_exposures(portfolio)

        for pair, side in new_pos:
            if pair in no_go_pairs:
                continue
            leg1, leg2 = pair.split('~')
            if (ern_flags[leg1] or ern_flags[leg2]) and remove_earnings:
                continue
            if self._check_pos_exposures(leg1, leg2, side, max_pos_count):
                portfolio.add_pair(pair, trade_prices,
                                   self.position_size, side)
        return

    def _get_pos_exposures(self, portfolio):
        """
        Get exposures each iteration.
        """
        self._exposures = {}
        for pos in portfolio.pairs.values():
            self._exposures[pos.leg1] = 0
            self._exposures[pos.leg2] = 0
        for pos in portfolio.pairs.values():
            if pos.open_position:
                self._exposures[pos.leg1] += 1 if pos.shares1 > 0 else -1
                self._exposures[pos.leg2] += 1 if pos.shares2 > 0 else -1

    def _check_pos_exposures(self, leg1, leg2, side, max_pos_count):
        """
        Returns true or false if the max position count has been hit.
        Side = 1 referse to going long the first pair, and short the second
        """
        if leg1 not in self._exposures:
            self._exposures[leg1] = 0
        if leg2 not in self._exposures:
            self._exposures[leg2] = 0
        exp1 = self._exposures[leg1] + side
        exp2 = self._exposures[leg2] - side
        if abs(exp1) <= max_pos_count:
            if abs(exp2) <= max_pos_count:
                self._exposures[leg1] += side
                self._exposures[leg2] -= side
                return True
            else:
                return False
            return False

    # ~~~~~~ Data Format ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_and_prep_data(self, data, pair_info, time_index):
        # Format new data
        self._data[time_index] = self._format_data(data, pair_info).copy()
        self.iter_dates = self._data[time_index]['iter_dates']
        # Pop old data
        if (time_index-2) in self._data:
            self._data.pop(time_index-2)
        # Put all data into one dictionary
        self.close_dict = {}
        self.dividend_dict = {}
        self.split_mult_dict = {}
        self.z_scores = {}
        self.earnings_flags = {}

        for t in self._data[time_index]['closes'].keys():
            tmp = self._data[time_index]
            self.close_dict[t] = tmp['closes'][t].copy()
            self.dividend_dict[t] = tmp['dividends'][t].copy()
            self.split_mult_dict[t] = tmp['split_mult'][t].copy()
            self.z_scores[t] = tmp['z_scores'][t].copy()
            self.earnings_flags[t] = tmp['earnings_flags'][t].copy()

        if (time_index-1) in self._data:
            for t in self._data[time_index-1]['closes'].keys():
                if t in self.close_dict.keys():
                    tmp = self._data[time_index-1]
                    self.close_dict[t].update(tmp['closes'][t])
                    self.dividend_dict[t].update(tmp['dividends'][t])
                    self.split_mult_dict[t].update(tmp['split_mult'][t])
                    self.z_scores[t].update(tmp['z_scores'][t])
                    self.earnings_flags[t].update(tmp['earnings_flags'][t])

        # Have new ZScores in separate dict
        self.new_z_scores = self._format_sort_new_z_scores(
            self._data[time_index]['z_scores'],
            self._data[time_index]['iter_dates'])

    def _format_data(self, data, pair_info):
        # Get training and test dates
        test_dates = data[data.TestFlag].Date.unique()
        qtrs = np.array([(x.month-1)/3+1 for x in test_dates])
        iter_dates = test_dates[qtrs == qtrs[0]]
        # Calculate State Variables, including Z-Score
        z_scores = self._get_zscores(data, pair_info, 100).loc[test_dates]
        closes = data.pivot(
            index='Date', columns='SecCode', values='RClose').loc[test_dates]
        dividends = data.pivot(
            index='Date', columns='SecCode',
            values='RCashDividend').fillna(0).loc[test_dates]
        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        split_mult = data.pivot(
            index='Date', columns='SecCode',
            values='SplitMultiplier').fillna(1).loc[test_dates]
        # Earnings Binaries
        # Add earnings flag to day before and day of to avoid entering
        # until T+1
        shift_seccode = data.SecCode.shift(-1)
        shift_ern_flag = data.EARNINGSFLAG.shift(-1)
        data['ERNFLAG'] = data.EARNINGSFLAG
        data.ERNFLAG += np.where(
            data.SecCode == shift_seccode, 1, 0) * shift_ern_flag
        earnings_flags = data.pivot(
            index='Date', columns='SecCode',
            values='ERNFLAG').fillna(0).astype(int).loc[test_dates]
        data = data.drop('ERNFLAG', axis=1)
        return {
            'iter_dates': iter_dates,
            'z_scores': z_scores.T.to_dict(),
            'closes': closes.T.to_dict(),
            'dividends': dividends.T.to_dict(),
            'split_mult': split_mult.T.to_dict(),
            'earnings_flags': earnings_flags.T.to_dict()
        }

    def _format_sort_new_z_scores(self, zscores, iter_dates):
        output = {}
        for d in iter_dates:
            tmp = [(abs(y), 1 if y < 0 else -1, x)
                   for x, y in zscores[d].items()]
            tmp.sort()
            output[d] = tmp[::-1]
        return output

    # ~~~~~~ Z-Score calculation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_zscores(self, data, pair_info, window):
        close_data = data.pivot(
            index='Date', columns='SecCode', values='AdjClose')
        # Create two data frames that represent Leg1 and Leg2
        close1 = close_data.loc[:, pair_info.Leg1]
        close2 = close_data.loc[:, pair_info.Leg2]
        spreads = pd.DataFrame(np.log(close1).values - np.log(close2).values)
        ma_df = spreads.rolling(window=window).mean()
        std_df = spreads.rolling(window=window).std()
        outdf = (spreads - ma_df) / std_df
        # Add correct column names
        outdf.index = close_data.index
        outdf.columns = ['{0}~{1}'.format(x, y) for x, y in
                         zip(pair_info.Leg1, pair_info.Leg2)]
        return outdf