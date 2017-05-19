import datetime as dt
import matplotlib.pyplot as plt

from ram.data.data_constructor_quandl import QuandlFuturesDataPull


# ~~~~~~ Import CME Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



## Stored as (Exchange, Symbol, Description)

# ~~~~~~ Precious Metals ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
codes = []
codes.append(('CME', 'GC', 'Gold'))


# ~~~~~~ Volatility ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

codes.append(('CBOE', 'VX', 'Volatility'))



qdata = QuandlFuturesDataPull(
    exchange='CBOE', contract='VX',
    start_year=2014, end_year=2017)
qdata.pull()


