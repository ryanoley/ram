import os
import numpy as np
import pandas as pd
import datetime as dt

from ram import config
from ram.data.data_handler_sql import DataHandlerSQL

from gearbox import convert_date_array


def get_trading_dates():
    """
    Returns previous trading date, and current trading date
    """
    today = dt.date.today()
    dh = DataHandlerSQL()
    dates = dh.prior_trading_date([today, today+dt.timedelta(days=1)])
    return dates[0], dates[1]


def get_bloomberg_file_prefix_date(
        file_name, imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    bloomberg_dir = os.path.join(imp_data_dir, 'bloomberg_data')
    all_files = os.listdir(bloomberg_dir)
    all_files = [x for x in all_files if x.find(file_name) > -1]
    prefix = max([x.split('_')[0] for x in all_files])
    return dt.date(int(prefix[:4]), int(prefix[4:6]), int(prefix[6:]))


def get_qadirect_file_prefix_dates():
    # QADIRECT - also cleans out old data
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    all_files.remove('market_index_data.csv')
    prefix = max([x.split('_')[0] for x in all_files])
    return dt.date(int(prefix[:4]), int(prefix[4:6]), int(prefix[6:]))


def get_qadirect_data_dates():
    raw_data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    all_files.remove('market_index_data.csv')
    max_date_prefix = max([x.split('_')[0] for x in all_files])
    # Read in dates for files
    todays_files = [x for x in all_files if x.find(max_date_prefix) > -1]
    max_dates = []
    for f in todays_files:
        data = pd.read_csv(os.path.join(raw_data_dir, f), nrows=3000)
        max_dates.append(data.Date.max())
    return todays_files, convert_date_array(max_dates)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def process_bloomberg_data(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    message = ''
    bloomberg = pd.DataFrame()
    output = _import_bloomberg_dividends(imp_data_dir)
    if isinstance(output, str):
        message += output
    else:
        bloomberg = bloomberg.append(output)

    output = _import_bloomberg_splits(imp_data_dir)
    if isinstance(output, str):
        message += output
    else:
        bloomberg = bloomberg.append(output)

    output = _import_bloomberg_spinoffs(imp_data_dir)
    if isinstance(output, str):
        message += output
    else:
        bloomberg = bloomberg.append(output)
    # Write bloomberg data to file
    bloomberg = bloomberg.groupby('Ticker')['Multiplier'].prod().reset_index()
    path = os.path.join(imp_data_dir, 'StatArbStrategy',
                        'live_pricing', 'bloomberg_scaling.csv')
    bloomberg.to_csv(path, index=None)
    return message


def _import_bloomberg_dividends(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    prefix_date = get_bloomberg_file_prefix_date('dividends', imp_data_dir)
    file_name = prefix_date.strftime('%Y%m%d') + '_dividends.csv'
    data = pd.read_csv(os.path.join(imp_data_dir, 'bloomberg_data', file_name))
    # Check columns
    columns = ['DPS Last Gross', 'Dvd Ex Dt', 'Market Cap', 'Market Cap#1',
               'P/E', 'Price:D-1', 'Short Name', 'Ticker']
    if not np.all(data.columns == columns):
        return "Dividend columns do not match"
    data.columns = ['CashDividend', 'ExDate', 'temp1', 'temp2', 'temp3',
                    'ClosePrice', 'temp4', 'Ticker']
    data['Multiplier'] = data.CashDividend / data.ClosePrice + 1
    data.ExDate = convert_date_array(data.ExDate)
    data = data[data.ExDate == dt.date.today()]
    data.Ticker = [x.replace(' US', '') for x in data.Ticker]
    data = data[['Ticker', 'Multiplier']]
    data = data[data.Multiplier != 1]
    return data.reset_index(drop=True).dropna()


def _import_bloomberg_splits(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    prefix_date = get_bloomberg_file_prefix_date('splits', imp_data_dir)
    file_name = prefix_date.strftime('%Y%m%d') + '_splits.csv'
    data = pd.read_csv(os.path.join(imp_data_dir, 'bloomberg_data', file_name))
    # Check columns
    columns = ['Current Stock Split Adjustment Factor', 'Market Cap',
               'Market Cap#1', 'P/E', 'Price:D-1', 'Short Name',
               'Stk Splt Ex Dt', 'Ticker']
    if not np.all(data.columns == columns):
        return "Split columns do not match"
    data.columns = ['Multiplier', 'temp1', 'temp2', 'temp3', 'temp4',
                    'temp5', 'SplitExDate', 'Ticker']
    data.SplitExDate = convert_date_array(data.SplitExDate)
    data = data[data.SplitExDate == dt.date.today()]
    data.Ticker = [x.replace(' US', '') for x in data.Ticker]
    data = data[['Ticker', 'Multiplier']]
    return data.reset_index(drop=True).dropna()


def _import_bloomberg_spinoffs(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    prefix_date = get_bloomberg_file_prefix_date('spinoffs', imp_data_dir)
    file_name = prefix_date.strftime('%Y%m%d') + '_spinoffs.csv'
    data = pd.read_csv(os.path.join(imp_data_dir, 'bloomberg_data', file_name))
    # Check columns
    columns = ['Market Cap', 'Market Cap#1', 'Price:D-1', 'Short Name',
               'Spin Adj Fact Curr', 'Spin Adj Fact Nxt',
               'Spinoff Ex Date', 'Ticker']
    if not np.all(data.columns == columns):
        return "Spinoff columns do not match"
    data.columns = ['temp1', 'temp2', 'temp3', 'temp4', 'SpinFactor', 'temp5',
                    'SpinExDate', 'Ticker']
    data.SpinExDate = convert_date_array(data.SpinExDate)
    data = data[data.SpinExDate == dt.date.today()]
    data.Ticker = [x.replace(' US', '') for x in data.Ticker]
    data['Multiplier'] = 1 / data.SpinFactor
    data = data[['Ticker', 'Multiplier']]
    return data.reset_index(drop=True).dropna()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():

    yesterday, today = get_trading_dates()

    output = pd.DataFrame()

    message = process_bloomberg_data()
    output.loc[0, 'Desc'] = 'Bloomberg processing'
    output.loc[0, 'Date'] = ''
    output.loc[0, 'Alert'] = message

    output.loc[1, 'Desc'] = 'Bloomberg Dividend File Prefix'
    date = get_bloomberg_file_prefix_date('dividends')
    output.loc[1, 'Date'] = date
    output.loc[1, 'Alert'] = '!!!' if date != today else ''

    output.loc[2, 'Desc'] = 'Bloomberg Spinoff File Prefix'
    date = get_bloomberg_file_prefix_date('spinoffs')
    output.loc[2, 'Date'] = date
    output.loc[2, 'Alert'] = '!!!' if date != today else ''

    output.loc[3, 'Desc'] = 'Bloomberg Splits File Prefix'
    date = get_bloomberg_file_prefix_date('splits')
    output.loc[3, 'Date'] = date
    output.loc[3, 'Alert'] = '!!!' if date != today else ''

    output.loc[4, 'Desc'] = 'QADirect File Prefix'
    date = get_qadirect_file_prefix_dates()
    output.loc[4, 'Date'] = date
    output.loc[4, 'Alert'] = '!!!' if date != today else ''

    ind = len(output)

    for n, d in zip(*get_qadirect_data_dates()):
        output.loc[ind, 'Desc'] = n
        output.loc[ind, 'Date'] = d
        output.loc[ind, 'Alert'] = '!!!' if d != yesterday else ''
        ind += 1

    # OUTPUT to file
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'pretrade_data_check.csv')
    output.to_csv(dpath, index=None)
    # Archive
    ddir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'pretrade_check_archive')
    if not os.path.isdir(ddir):
        os.mkdir(ddir)
    dpath = os.path.join(ddir, 'pretrade_data_check_{}.csv'.format(
        today.strftime('%Y%m%d')))
    output.to_csv(dpath, index=None)


if __name__ == '__main__':
    main()
