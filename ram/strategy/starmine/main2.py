import itertools
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.starmine.signals.signals1 import SignalModel1
from ram.strategy.starmine.data.data_container1 import DataContainer1
from ram.strategy.starmine.constructor.constructor1 import PortfolioConstructor1


class PostErnStrategy(Strategy):

    data = DataContainer1()
    signals = SignalModel1()
    constructor = PortfolioConstructor1(5e6)

    def get_column_parameters(self):
        """
        These are written to file
        """
        args1 = make_arg_iter(self.data.get_args())
        args2 = make_arg_iter(self.signals.get_args())
        args3 = make_arg_iter(self.constructor.get_args())

        output_params = {}
        for col_ind, (x, y, z) in enumerate(itertools.product(args1,
                                                              args2,
                                                              args3)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

    def run_index(self, time_index):

        # Import, process, and stack data
        self.data.add_data(self.read_data_from_index(time_index))

        if len(self.data._processed_train_data) == 0:
            return
        if len(self.data._processed_test_data) == 0:
            return        

        # Restart Functionality: check if file already run.
        if time_index <= self._max_run_time_index:
            return
        if time_index < 8:
            return

        args_data = make_arg_iter(self.data.get_args())
        args_signals = make_arg_iter(self.signals.get_args())
        args_constructor = make_arg_iter(self.constructor.get_args())

        i = 0
        for ad in args_data:

            self.data.prep_data(**ad)

            for as_ in args_signals:

                self.signals.rf_signals(self.data, **as_)

                for ac in args_constructor:
    
                    result, stats = self.constructor.get_daily_pl(self.data,
                                                                  self.signals,
                                                                  **ac)
                    self._capture_output(result, stats, i)
                    i += 1

        self.write_index_results(self.output_returns, time_index)
        self.write_index_results(self.output_all_output, time_index,
                                 'all_output')
        self.write_index_stats(self.output_stats, time_index)

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, stats, arg_index):
        returns = pd.DataFrame(results.PL / self.constructor.booksize)
        returns.columns = [arg_index]
        # Rename columns
        results.columns = ['{}_{}'.format(x, arg_index)
                           for x in results.columns]
        if arg_index == 0:
            self.output_returns = returns
            self.output_all_output = results
            self.output_stats = {}
        else:
            self.output_returns = self.output_returns.join(returns,
                                                           how='outer')
            self.output_all_output = self.output_all_output.join(
                results, how='outer')
        self.output_stats[arg_index] = stats

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_univ_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 50 and GSECTOR = 45 ' +
            'and Close_ >= 15 and AvgDolVol >= 1',
            'univ_size': 300
        }

    def get_univ_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 1,
            'test_period_length': 1,
            'start_year': 2003
        }

    def get_features(self):
        return [
            # Descriptive
            'TM1', 'T1', 'GGROUP', 'EARNINGSRETURN', 'EARNINGSFLAG',
            'MarketCap', 'AvgDolVol', 'SplitFactor', 'RVwap', 'RClose',
            'RCashDividend',

            # Pricing
            'AdjOpen', 'AdjClose', 'AdjVwap', 'LEAD1_AdjVwap',
            'LEAD20_AdjVwap', 'LEAD21_AdjVwap','LEAD22_AdjVwap',
            'LEAD23_AdjVwap',

            # Pricing Features
            'PRMA10_AdjClose', 'PRMA20_AdjClose', 'PRMA60_AdjClose',
            'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
            'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose',

            # Accounting Features
            'NETINCOMEQ', 'NETINCOMETTM',
            'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',

            'OPERATINGINCOMEQ', 'OPERATINGINCOMETTM',
            'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',

            'EBITQ', 'EBITTTM',
            'EBITGROWTHQ', 'EBITGROWTHTTM',

            'SALESQ', 'SALESTTM',
            'SALESGROWTHQ', 'SALESGROWTHTTM',

            'FREECASHFLOWQ', 'FREECASHFLOWTTM',
            'FREECASHFLOWGROWTHQ', 'FREECASHFLOWGROWTHTTM',

            'ADJEPSQ', 'ADJEPSTTM',
            'ADJEPSGROWTHQ', 'ADJEPSGROWTHTTM',

            # Starmine Features
            'EPSESTIMATEFQ1', 'EPSESTIMATEFQ2', 'EBITDAESTIMATEFQ1',
            'EBITDAESTIMATEFQ2', 'REVENUEESTIMATEFQ1', 'REVENUEESTIMATEFQ2',

            # Price Targets
            'PTARGETMEAN', 'PTARGETHIGH', 'PTARGETLOW', 'PTARGETUNADJ',
            'RECMEAN'
        ]


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(PostErnStrategy)
