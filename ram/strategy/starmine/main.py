import itertools
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.starmine.data.data_container1 import DataContainer1
from ram.strategy.starmine.constructor.constructor1 import \
    PortfolioConstructor1

from ram.strategy.starmine.signals.signals1 import SignalModel1


class StarmineStrategy(Strategy):

    data = DataContainer1()
    signals = SignalModel1()
    constructor = PortfolioConstructor1()

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

        # Restart Functionality: check if file already run.
        if time_index <= self._max_run_time_index:
            return

        args_data = make_arg_iter(self.data.get_args())
        args_signals = make_arg_iter(self.signals.get_args())
        args_constructor = make_arg_iter(self.constructor.get_args())

        i = 0
        for ad in args_data:

            self.data.prep_data(**ad)

            for as_ in args_signals:

                self.signals.generate_signals(self.data, **as_)

                for ac in args_constructor:

                    result = self.constructor.get_daily_pl(
                        self.data, self.signals, **ac)

                    self._capture_output(result, i)
                    i += 1

        self.write_index_results(self.output_returns, time_index)
        self.write_index_results(self.output_statistics, time_index,
                                 'all_output')

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, results, arg_index):
        returns = pd.DataFrame(results.PL / self.constructor.booksize)
        returns.columns = [arg_index]
        # Rename columns
        results.columns = ['{}_{}'.format(x, arg_index)
                           for x in results.columns]
        if arg_index == 0:
            self.output_returns = returns
            self.output_statistics = results
        else:
            self.output_returns = self.output_returns.join(returns,
                                                           how='outer')
            self.output_statistics = self.output_statistics.join(
                results, how='outer')

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_univ_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 ' +
            'and Close_ >= 15',
            'univ_size': 500
        }

    def get_univ_date_parameters(self):
        return {
            'frequency': 'Q',
            'train_period_length': 1,
            'test_period_length': 1,
            'start_year': 2002
        }

    def get_features(self):
        return [
            # Descriptive
            'GGROUP', 'EARNINGSFLAG', 'MarketCap', 'AvgDolVol',
            
            'AdjClose', 'AdjVwap', 'AdjOpen', 'SplitFactor',

            # Starmine Features
            'ARM', 'ARMREVENUE', 'ARMRECS', 'ARMEARNINGS', 'ARMEXRECS',
            'EPSESTIMATE', 'EPSSURPRISE', 'EBITDAESTIMATE', 'EBITDASURPRISE',
            'REVENUEESTIMATE', 'REVENUESURPRISE', 'SESPLITFACTOR',
            'SIRANK', 'SIMARKETCAPRANK', 'SISECTORRANK',
            'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP'
        ]


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(StarmineStrategy)