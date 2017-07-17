'''
IQ FEED MESSAGE FORMAT:
CMD,SYM,[options]\n

Historical data (HIT) options:
[bars in seconds],[beginning date: CCYYMMDD HHmmSS],
[ending date: CCYYMMDD HHmmSS],[empty],[beginning time filter: HHmmSS],
[ending time filter: HHmmSS],[old or new: 0 or 1],[empty],
[queue data points per second]

sample command for 1 min bars during trading hrs:
"HIT,GOOG,60,20140101 075000,,,093000,160000,1\n

data format returned from iqfeed:
[YYYY-MM-DD HH:mm:SS],[HIGH],[LOW],[OPEN],[CLOSE],[VOLUME],[OPEN INTEREST]
'''

import socket
import time
import os
import argparse
import pandas as pd
import numpy as np
import datetime as dt
import pypyodbc
from StringIO import StringIO
from ram.data.data_handler_sql import DataHandlerSQL


INTRADAY_DIR = os.path.join(os.getenv('DATA'), 'ram', 'intraday_src')
IQF_COLS = ['DateTime', 'High', 'Low', 'Open', 'Close','Volume',
            'OpenInterest']


class IntradayDataManager(object):

    def __init__(self, src_dir = INTRADAY_DIR):
        self._src_dir = src_dir
        self.tickers = self.get_available_tickers()

    def get_available_tickers(self):
        try:
            return [x.strip('.csv') for x in os.listdir(self._src_dir)
                    if x.find('.csv') > 0]
        except:
            return 'No source files or directory found'

    def get_historical_data(self, tickers, interval=60):
        if not isinstance(tickers, list):
            tickers = [tickers]
    
        # Download each symbol to disk
        for ticker in tickers:
            if ticker in self.tickers:
                print "History already exists for %s" % ticker
                continue
            print "Downloading history data: %s..." % ticker

            # IQ Feed Message
            message = "HIT,{0},{1},20070101 075000,,,093000,160000,1\n".format(
                ticker, interval)
            data = self._iqf_request(message)
            data.columns = IQF_COLS

            if len(data) < 100:
                continue
            else:
                data['Ticker'] = ticker
                data = data[['Ticker'] + IQF_COLS]
                data.to_csv(os.path.join(self._src_dir, "%s.csv" % ticker),
                            index=False)
                time.sleep(10)
        return

    def update_data(self, tickers=None, interval=60):
        if tickers is None:
            tickers = self.tickers
        elif not isinstance(tickers, list):
            tickers = [tickers]
        assert(set(tickers).issubset(self.tickers))

        dh = DataHandlerSQL()
        prior_trading_dt = dh.prior_trading_date()

        for ticker in tickers:
            min_dt, max_dt = self._get_ticker_min_max_date(ticker)
            if max_dt.date() < prior_trading_dt:
                print "Downloading incremental data: %s..." % ticker

                increment = self._get_incremental_data(ticker,
                                                       max_dt,
                                                       interval)
                increment['Ticker'] = ticker
                csv_path = os.path.join(self._src_dir, "%s.csv" % ticker)
                history = pd.read_csv(csv_path)
                new_data = history.append(increment)
                new_data = new_data[['Ticker'] + IQF_COLS]
                new_data.to_csv(csv_path, index=False)
        return

    def _get_incremental_data(self, ticker, start_datetime, interval=60):
        if not isinstance(start_datetime, dt.datetime):
            start_datetime = parser.parse(start_datetime)
        
        if (start_datetime.time() < dt.time(16,00)):
            iqf_date = start_datetime.strftime(format = '%Y%m%d %H%M%S')
        else:
            iqf_date = start_datetime + dt.timedelta(1)
            iqf_date = iqf_date.strftime(format = '%Y%m%d  075000')

        # IQ Feed Message
        message = "HIT,{0},{1},{2},,,093000,160000,1\n".format(ticker,
                                                               interval,
                                                               iqf_date)
        data = self._iqf_request(message)
        data.columns = IQF_COLS
        return data

    def _iqf_request(self, message):
        # Define server host, port and symbols to download
        host = "127.0.0.1"  # Localhost
        port = 9100  # Historical data socket port
        # Open a streaming socket to the IQFeed server locally
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
    
        # Send the historical data request and buffer the data
        sock.sendall(message)
        data = self._read_from_socket(sock)
        sock.close()
        # Remove all the endlines and line-ending comma delimiter
        data = data.replace("\r", "")
        data = data.replace(",\n","\n")[:-1]

        # Write the data stream to output_dir
        dataIO = StringIO(data)
        df = pd.read_csv(dataIO, header=None)
        return df

    def _read_from_socket(self, sock, recv_buffer=4096):
        """
        Read the information from the socket, in a buffered
        fashion, receiving only 4096 bytes at a time.
    
        Parameters:
        sock - The socket object
        recv_buffer - Amount in bytes to receive per read
        """
        buffer = ""
        data = ""
        while True:
            data = sock.recv(recv_buffer)
            buffer += data
    
            # Check if the end message string arrives
            if "!ENDMSG!" in buffer:
                break
       
        # Remove the end message string
        buffer = buffer[:-12]
        return buffer

    def _get_ticker_min_max_date(self, ticker):
        assert(ticker) in self.tickers
        ticker_path = os.path.join(self._src_dir, "%s.csv" % ticker)
        data = pd.read_csv(ticker_path)
        data.DateTime = pd.to_datetime(data.DateTime)
        min_dt = data.DateTime.min()
        max_dt = data.DateTime.max()
        return (min_dt.to_datetime(), max_dt.to_datetime())

    def write_to_db(self, fl_path):
        data = pd.read_csv(fl_path)

        # Clean and prep for db entry
        db_cols = ['Ticker', 'DateTime', 'High', 'Low', 'Open', 'Close','Volume',
                   'OpenInterest']
        data = data[db_cols]
        data = data.where((pd.notnull(data)), None)
        data.to_csv(fl_path, index=False)
        data = [tuple(x) for x in data.values]
    
        # Create generic insert statement for new records
        fld_txt = '?,' * len(db_cols)
        SQLCommand = ("INSERT INTO ram.dbo.IntradayPricing" +
                      " VALUES (" + fld_txt[:-1] + ")")
        connection = pypyodbc.connect('Driver={SQL Server};Server=QADIRECT;'
                                   'Database=ram;uid=ramuser;pwd=183madison')
        cursor = connection.cursor()
        cursor.executemany(SQLCommand, data)
        connection.commit()
        connection.close()
        return



if __name__ == "__main__":

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-history', '--history', action='store_true',
        help='Get full history')
    parser.add_argument(
        '-u', '--update', action='store_true',
        help='Update tickers in src_dir')
    args = parser.parse_args()
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    idm = IntradayDataManager(INTRADAY_DIR)
    
    if args.history:
        idm.get_historical_data(['SPY','IWM'])
    elif args.update:
        idm.update_data()




