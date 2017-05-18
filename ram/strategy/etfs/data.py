import datetime as dt
import pandas_datareader as pdr

from ram.strategy.etfs.config import ETFS


class ETFData(object):

    def __init__(self, symbols=ETFS, start_date=dt.datetime(2000, 1, 1)):
        if isinstance(symbols, dict):
            self.descriptions = symbols.values()
            symbols = symbols.keys()
        elif isinstance(symbols, str):
            symbols = [symbols]
        self.symbols = symbols
        self.data = pdr.get_data_yahoo(symbols=symbols, start=start_date)
        # Rename adjust close column
        data_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']
        self.data.items = data_cols
        # Make data accessible by ticker instead of data?
        # self.data = self.data.swapaxes(0, 2)
