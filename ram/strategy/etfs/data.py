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
        
        data = pdr.get_data_google(symbols=symbols, start=start_date)
        self.data = data.to_frame().reset_index()
        self.data.rename(columns={'minor': 'Ticker'}, inplace=True)
