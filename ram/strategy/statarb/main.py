import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base2 import Strategy2

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy2):

    def get_column_parameters(self):
        return []

    def run_index(self, index):

        data = self.read_data_from_index(index)

        pairselector = PairsStrategy1()
        constructor = PortfolioConstructor()

        args1 = make_arg_iter(pairselector.get_iterable_args())
        args2 = make_arg_iter(constructor.get_iterable_args())

        ind = 0
        output_results = pd.DataFrame()
        output_params = {}
        output_stats = {}

        for a1 in args1:
            scores, pair_info = pairselector.get_best_pairs(
                data, cut_date, **a1)

            # Optimization
            constructor.set_and_prep_data(scores, pair_info, data)

            for a2 in args2:
                results, stats = self.constructor.get_daily_pl(**a2)
                results.columns = [ind]
                output_results = output_results.join(results, how='outer')
                output_stats[ind] = stats
                ind += 1

        self.write_index_results(output_results, index)
        self.write_index_stats(output_stats, index)


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    strategy = StatArbStrategy(False)
    strategy.start()
