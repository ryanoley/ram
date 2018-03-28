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
    todays_files = [x for x in all_files if x.find('version') > -1]
    todays_files = [x for x in todays_files if x.find(max_date_prefix) > -1]
    max_dates = []
    for f in todays_files:
        data = pd.read_csv(os.path.join(raw_data_dir, f), nrows=3000)
        max_dates.append(data.Date.max())
    return todays_files, convert_date_array(max_dates)


def get_universe_seccodes(imp_dir=config.IMPLEMENTATION_DATA_DIR):
    raw_data_dir = os.path.join(imp_dir,
                                'StatArbStrategy', 'daily_raw_data')
    all_files = os.listdir(raw_data_dir)
    try:
        all_files.remove('market_index_data.csv')
    except:
        pass
    max_date_prefix = max([x.split('_')[0] for x in all_files])
    # Read in dates for files
    todays_files = [x for x in all_files if x.find('version') > -1]
    todays_files = [x for x in todays_files if x.find(max_date_prefix) > -1]
    all_seccodes = []
    for f in todays_files:
        data = pd.read_csv(os.path.join(raw_data_dir, f))
        all_seccodes += data.SecCode.unique().tolist()
    return np.unique(all_seccodes)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def process_bloomberg_data(imp_data_dir=config.IMPLEMENTATION_DATA_DIR,
                           mapping=None):
    message = ''
    bloomberg = pd.DataFrame()

    # DIVIDENDS
    try:
        output = _import_bloomberg_dividends(imp_data_dir)
        bloomberg = output
        inds = np.abs(output.DivMultiplier - 1) > .1
        if np.any(inds):
            message += 'Spotcheck dividend multiplier {}; '.format(
                output.Ticker[inds].tolist())
    except Exception as e:
        message += 'Dividends: {}; '.format(e.__repr__())

    # SPINOFFS
    try:
        output = _import_bloomberg_spinoffs(imp_data_dir)
        bloomberg = bloomberg.merge(output, how='outer')
        inds = (output.SpinoffMultiplier < .1) | \
            (output.SpinoffMultiplier > 10)
        if np.any(inds):
            message += 'Spotcheck spinoff multiplier {}; '.format(
                output.Ticker[inds].tolist())
    except Exception as e:
        message += 'Spinoffs: {}; '.format(e.__repr__())

    # SPLITS
    try:
        output = _import_bloomberg_splits(imp_data_dir)
        bloomberg = bloomberg.merge(output, how='outer')
        inds = (output.SplitMultiplier < .1) | \
                np.any(output.SplitMultiplier > 10)
        if np.any(inds):
            message += 'Spotcheck split multiplier {}; '.format(
                output.Ticker[inds].tolist())
    except Exception as e:
        message += 'Splits: {}; '.format(e.__repr__())

    # Fill nans with 1 so they don't change prices downstream
    bloomberg = bloomberg.fillna(1)

    # MERGE HERE WITH TICKERS
    univ = pd.DataFrame({'SecCode': get_universe_seccodes(imp_data_dir)})
    univ.SecCode = univ.SecCode.astype(str)

    # For testing
    if not np.any(mapping):
        dh = DataHandlerSQL()
        mapping = dh.get_ticker_seccode_map()
        mapping.Cusip = mapping.Cusip.astype(str)

    mapping = univ.merge(mapping, how='left')

    mapping1 = \
        mapping[['SecCode', 'Ticker', 'Issuer']].merge(bloomberg, how='left')
    mapping2 = \
        mapping[['SecCode', 'Cusip', 'Issuer']].merge(bloomberg, how='left')

    output = mapping1.dropna().append(mapping2.dropna()).drop_duplicates()
    output = output[['SecCode', 'DivMultiplier',
                     'SpinoffMultiplier', 'SplitMultiplier']]

    # Write bloomberg data to file
    path = os.path.join(imp_data_dir, 'StatArbStrategy',
                        'live_pricing', 'bloomberg_scaling.csv')
    output.to_csv(path, index=None)

    d = dt.date.today().strftime('%Y%m%d')
    file_name = '{}_bloomberg_scaling.csv'.format(d)
    path = os.path.join(imp_data_dir, 'StatArbStrategy',
                        'daily_raw_data', file_name)
    output.to_csv(path, index=None)
    if len(message) == 0:
        message = '*'
    return message


def _import_bloomberg_dividends(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    prefix_date = get_bloomberg_file_prefix_date('dividends', imp_data_dir)
    file_name = prefix_date.strftime('%Y%m%d') + '_dividends.csv'
    data = pd.read_csv(os.path.join(imp_data_dir, 'bloomberg_data', file_name))
    # Check columns
    columns = ['CUSIP', 'DPS Last Gross', 'Dvd Ex Dt', 'Market Cap',
               'Market Cap#1', 'Price:D-1', 'Short Name', 'Ticker']
    if not np.all(data.columns == columns):
        raise Exception("Dividend columns do not match")
    data.columns = ['Cusip', 'CashDividend', 'ExDate', 'temp1', 'temp2',
                    'ClosePrice', 'temp4', 'Ticker']

    data['DivMultiplier'] = data.CashDividend / data.ClosePrice + 1
    data.ExDate = convert_date_array(data.ExDate)
    data = data[data.ExDate == dt.date.today()]
    data.Ticker = data.Ticker.apply(lambda x: x.replace(' US', ''))
    data.Cusip = data.Cusip.astype(str).apply(lambda x: x[:8])
    data = data[['Ticker', 'Cusip', 'DivMultiplier']]
    data = data[data.DivMultiplier != 1]
    return data.reset_index(drop=True).dropna()


def _import_bloomberg_splits(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    prefix_date = get_bloomberg_file_prefix_date('splits', imp_data_dir)
    file_name = prefix_date.strftime('%Y%m%d') + '_splits.csv'
    data = pd.read_csv(os.path.join(imp_data_dir, 'bloomberg_data', file_name))
    # Check columns
    columns = ['CUSIP', 'Current Stock Split Adjustment Factor',
               'Market Cap', 'Next Stock Split Ratio', 'Short Name',
               'Stk Splt Ex Dt', 'Ticker']
    if not np.all(data.columns == columns):
        raise Exception("Split columns do not match")
    data.columns = ['Cusip', 'temp1', 'temp2', 'SplitMultiplier',
                    'temp4', 'SplitExDate', 'Ticker']
    data.SplitExDate = convert_date_array(data.SplitExDate)
    data = data[data.SplitExDate == dt.date.today()]
    data.Ticker = data.Ticker.apply(lambda x: x.replace(' US', ''))
    data.Cusip = data.Cusip.astype(str).apply(lambda x: x[:8])
    data = data[['Ticker', 'Cusip', 'SplitMultiplier']]
    return data.reset_index(drop=True).dropna()


def _import_bloomberg_spinoffs(imp_data_dir=config.IMPLEMENTATION_DATA_DIR):
    prefix_date = get_bloomberg_file_prefix_date('spinoffs', imp_data_dir)
    file_name = prefix_date.strftime('%Y%m%d') + '_spinoffs.csv'
    data = pd.read_csv(os.path.join(imp_data_dir, 'bloomberg_data', file_name))
    # Check columns
    columns = ['CUSIP', 'CUSIP#1', 'Market Cap', 'Short Name',
               'Spin Adj Fact Curr', 'Spin Adj Fact Nxt',
               'Spinoff Ex Date', 'Ticker']
    if not np.all(data.columns == columns):
        raise Exception("Spinoff columns do not match")
    data.columns = ['Cusip', 'temp1', 'temp2', 'temp3', 'temp4',
                    'SpinFactor', 'SpinExDate', 'Ticker']
    data.SpinExDate = convert_date_array(data.SpinExDate)
    data = data[data.SpinExDate == dt.date.today()]
    data.Ticker = data.Ticker.apply(lambda x: x.replace(' US', ''))
    data.Cusip = data.Cusip.astype(str).apply(lambda x: x[:8])
    data['SpinoffMultiplier'] = 1 / data.SpinFactor
    data = data[['Ticker', 'Cusip', 'SpinoffMultiplier']]
    return data.reset_index(drop=True).dropna()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _check_date(date, today):
    return '[WARNING] - Not up-to-date' if date != today else '*'


def main():

    yesterday, today = get_trading_dates()

    output = pd.DataFrame()

    message = process_bloomberg_data()
    output.loc[0, 'Desc'] = 'Bloomberg processing'
    output.loc[0, 'Message'] = message

    output.loc[1, 'Desc'] = 'Bloomberg Dividend File Prefix'
    date = get_bloomberg_file_prefix_date('dividends')
    output.loc[1, 'Message'] = _check_date(date, today)

    output.loc[2, 'Desc'] = 'Bloomberg Spinoff File Prefix'
    date = get_bloomberg_file_prefix_date('spinoffs')
    output.loc[2, 'Message'] = _check_date(date, today)

    output.loc[3, 'Desc'] = 'Bloomberg Splits File Prefix'
    date = get_bloomberg_file_prefix_date('splits')
    output.loc[3, 'Message'] = _check_date(date, today)

    output.loc[4, 'Desc'] = 'QADirect File Prefix'
    date = get_qadirect_file_prefix_dates()
    output.loc[4, 'Message'] = _check_date(date, today)

    ind = len(output)

    for desc, last_date in zip(*get_qadirect_data_dates()):
        desc = 'Raw data last date: ' + desc[desc.find('version'):]
        output.loc[ind, 'Desc'] = desc
        output.loc[ind, 'Message'] = _check_date(last_date, yesterday)
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
