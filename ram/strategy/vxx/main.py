import os
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy


class VXXStrategy(Strategy):

    def get_column_parameters(self):
        return []

    def run_index(self, time_index):
        data = pdr.get_data_google('VXX', start=dt.datetime(2009, 1, 1))


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-w', '--write_simulation', action='store_true',
        help='Run simulation')
    parser.add_argument(
        '-s', '--simulation', action='store_true',
        help='Run simulation')
    args = parser.parse_args()

    if args.write_simulation:
        strategy = VXXStrategy(write_flag=True)
        strategy.start()
    elif args.simulation:
        strategy = VXXStrategy(write_flag=False)
        import pdb; pdb.set_trace()
        strategy.start()
