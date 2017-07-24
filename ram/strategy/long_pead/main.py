import itertools
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.strategy.long_pead.constructor.constructor import PortfolioConstructor
from ram.strategy.long_pead.constructor.constructor2 import PortfolioConstructor2
from ram.strategy.long_pead.constructor.constructor3 import PortfolioConstructor3
from ram.strategy.long_pead.constructor.constructor4 import PortfolioConstructor4


class LongPeadStrategy(Strategy):

    constructor = PortfolioConstructor2()

    def get_column_parameters(self):
        """
        These are written to file
        """
        args1 = make_arg_iter(self.constructor.get_data_args())
        args2 = make_arg_iter(self.constructor.get_iterable_args())
        output_params = {}
        for col_ind, (x, y) in enumerate(itertools.product(args1, args2)):
            params = dict(x)
            params.update(y)
            output_params[col_ind] = params
        return output_params

    def run_index(self, time_index):
        data = self.read_data_from_index(time_index)
        args_port = make_arg_iter(self.constructor.get_iterable_args())
        args_data = make_arg_iter(self.constructor.get_data_args())
        i = 0
        for ad in args_data:
            self.constructor.set_and_prep_data(data, time_index, **ad)
            for ap in args_port:
                result = self.constructor.get_daily_pl(arg_index=i, **ap)
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
        results.columns = ['{}_{}'.format(x, arg_index) for x in results.columns]
        if arg_index == 0:
            self.output_returns = returns
            self.output_statistics = results
        else:
            self.output_returns = self.output_returns.join(returns, how='outer')
            self.output_statistics = self.output_statistics.join(
                results, how='outer')

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_univ_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR in (15) ' +
            'and Close_ between 5 and 500',
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
            'AdjClose', 'AdjVwap',
            'LEAD1_ROpen', 'RClose', 'RCashDividend', 'SplitFactor',

            # Descriptive
            'GGROUP', 'EARNINGSFLAG', 'MarketCap', 'AvgDolVol',

            # Pricing Features
            'PRMA120_AvgDolVol',
            'PRMA10_AdjClose', 'PRMA20_AdjClose',
            'BOLL10_AdjClose', 'BOLL20_AdjClose', 'BOLL60_AdjClose',
            'MFI10_AdjClose', 'MFI20_AdjClose', 'MFI60_AdjClose',
            'RSI10_AdjClose', 'RSI20_AdjClose', 'RSI60_AdjClose',
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

            'GROSSPROFASSET',
            'ASSETS',

            'GROSSMARGINTTM',
            'EBITDAMARGIN',

            'PE', 'FCFMARKETCAP', 'CASHEV',
        ]


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(LongPeadStrategy)
