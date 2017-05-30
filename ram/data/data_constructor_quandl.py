import os
import quandl
import itertools
import pandas as pd
from tqdm import tqdm

quandl.ApiConfig.api_key = 'd4jQsZQHxXyxP69WzBsK'

month_codes = [
    ('F', 'JAN'),
    ('G', 'FEB'),
    ('H', 'MAR'),
    ('J', 'APR'),
    ('K', 'MAY'),
    ('M', 'JUN'),
    ('N', 'JUL'),
    ('Q', 'AUG'),
    ('U', 'SEP'),
    ('V', 'OCT'),
    ('X', 'NOV'),
    ('Z', 'DEC')
]


class QuandlFuturesDataPull(object):

    def __init__(self, exchange, contract, start_year=2010, end_year=2017):
        self._codes = []
        self._labels = []
        for year, month in itertools.product(
                range(start_year, end_year+1), month_codes):
            self._codes.append('{}/{}{}{}'.format(
                exchange, contract, month[0], year))
            self._labels.append('{}{}'.format(year, month[1]))
        # Formatting is different by exchange
        if exchange == 'CME':
            self._data_columns = [5, 6, 7]
        elif exchange == 'CBOE':
            self._data_columns = [3, 6, 8]
        else:
            raise Exception('Exchange not known')
        return

    def pull(self):
        self.data = {}
        close = pd.DataFrame()
        volume = pd.DataFrame()
        openinterest = pd.DataFrame()
        for code, label in tqdm(zip(self._codes, self._labels)):
            try:
                data = quandl.get(code)
                data = data.iloc[:, self._data_columns]
                data.columns = ['Close', 'Volume', 'OI']
            except:
                continue
            close = close.join(
                pd.Series(data.Close, name=label),
                how='outer')
            volume = volume.join(
                pd.Series(data.Volume, name=label),
                how='outer')
            openinterest = openinterest.join(
                pd.Series(data.OI, name=label),
                how='outer')
        self.data['close'] = close
        self.data['volume'] = volume
        self.data['openinterest'] = openinterest
