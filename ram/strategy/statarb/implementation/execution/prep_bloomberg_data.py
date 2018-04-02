# Bloomberg to QADirect mapping
import os
import json
import numpy as np
import pandas as pd
import datetime as dt

from ram import config

from gearbox import convert_date_array


def get_todays_date_prefix():
    today = dt.date.today()
    return '{:02d}{:02d}{:02d}'.format(today.year, today.month, today.day)


def map_seccodes_bloomberg_tickers():

    prefix = get_todays_date_prefix()

    # Import Bloomberg Ticker Mapping Files
    bloomberg_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                 'bloomberg_data')
    dpath1 = os.path.join(bloomberg_dir, '{}_ticker_cusip.csv'.format(prefix))
    dpath2 = os.path.join(bloomberg_dir, '{}_ticker_cusip2.csv'.format(prefix))
    data1 = pd.read_csv(dpath1)
    data2 = pd.read_csv(dpath2)

    # Process Bloomberg Ticker Mapping Files
    data = data1[['CUSIP', 'Ticker']] \
        .append(data2[['CUSIP', 'Ticker']]).reset_index(drop=True)
    data.columns = ['BloombergCusip', 'BloombergId']
    data.BloombergCusip = data.BloombergCusip.astype(str)
    data.BloombergCusip = [x[:8] for x in data.BloombergCusip]
    data['Ticker'] = data.BloombergId.apply(lambda x: x.replace(' US', ''))

    # Import QAD Ticke Mapping
    data_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                            'StatArbStrategy', 'daily_data')
    dpath1 = os.path.join(data_dir, '{}_ticker_mapping.csv'.format(prefix))
    qad_map = pd.read_csv(dpath1)
    qad_map = qad_map[~qad_map.Ticker.isin(['$SPX.X', '$VIX.X'])]

    # Import Odd Ticker HashMap
    dpath = os.path.join(bloomberg_dir, 'odd_bloomberg_ticker_hash.json')
    hash_map = json.load(open(dpath, 'r'))

    # Merge
    qad_map = qad_map.merge(data, how='left', on='Ticker')

    # Fill in manually handled values from hash_map
    for k, v in hash_map.iteritems():
        ind = qad_map[qad_map.Ticker == k].index[0]
        qad_map.loc[ind, 'BloombergId'] = v + ' US'

    qad_map = qad_map[['SecCode', 'BloombergId']]

    return qad_map


def import_bloomberg_dividends():

    prefix = get_todays_date_prefix()

    # Import Bloomberg Ticker Mapping Files
    bloomberg_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                 'bloomberg_data')
    dpath = os.path.join(bloomberg_dir, '{}_dividends.csv'.format(prefix))
    data = pd.read_csv(dpath)

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Dvd Ex Dt'])
    data = data[data.ExDate == dt.date.today()]

    # Calculate DivMultiplier
    data['DivMultiplier'] = data['DPS Last Gross'] / data['Price:D-1'] + 1
    data = data[data.DivMultiplier != 1]

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'DivMultiplier']]

    return data.reset_index(drop=True).dropna()


def import_bloomberg_spinoffs():

    prefix = get_todays_date_prefix()

    # Import Bloomberg Ticker Mapping Files
    bloomberg_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                 'bloomberg_data')
    dpath = os.path.join(bloomberg_dir, '{}_spinoffs.csv'.format(prefix))
    data = pd.read_csv(dpath)

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Spinoff Ex Date'])
    data = data[data.ExDate == dt.date.today()]

    # Calculate SpinoffMultiplier
    data['SpinoffMultiplier'] = 1 / data['Spin Adj Fact Curr']

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'SpinoffMultiplier']]

    return data.reset_index(drop=True).dropna()


def import_bloomberg_splits():

    prefix = get_todays_date_prefix()

    # Import Bloomberg Ticker Mapping Files
    bloomberg_dir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                                 'bloomberg_data')
    dpath = os.path.join(bloomberg_dir, '{}_splits.csv'.format(prefix))
    data = pd.read_csv(dpath)

    # Select relevant date, which is today
    data['ExDate'] = convert_date_array(data['Stk Splt Ex Dt'])
    data = data[data.ExDate == dt.date.today()]

    # Calculate SpinoffMultiplier
    # Next Stock Split Ratio??
    data['SplitMultiplier'] = data['Current Stock Split Adjustment Factor']

    # Rename and select columns
    data['BloombergId'] = data['Ticker']
    data = data[['BloombergId', 'SplitMultiplier']]

    return data.reset_index(drop=True).dropna()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def process_bloomberg_data():

    message = []
    bloomberg = pd.DataFrame()

    qad_map = map_seccodes_bloomberg_tickers()
    # Check for missing codes
    flags = qad_map.BloombergId.isnull().sum()
    if flags > 0:
        message.append('[{}] SecCodes missing Bloomberg ids'.format(flags))
    else:
        message.append('*')

    # DIVIDENDS
    try:
        divs = import_bloomberg_dividends()
        flags = np.abs(divs.DivMultiplier - 1) > .1
        if np.any(flags):
            message.append('Spotcheck dividend multiplier')
        else:
            message.append('*')
    except Exception as e:
        message.append('Dividends: {}; '.format(e.__repr__()))

    # SPINOFFS
    try:
        spins = import_bloomberg_spinoffs()
        flags = (spins.SpinoffMultiplier < .1) | \
            (spins.SpinoffMultiplier > 10)
        if np.any(flags):
            message.append('Spotcheck spinoff multiplier')
        else:
            message.append('*')
    except Exception as e:
        message += 'Spinoffs: {}; '.format(e.__repr__())

    # SPLITS
    try:
        splits = import_bloomberg_splits()

        flags = (splits.SplitMultiplier < .1) | \
            (splits.SplitMultiplier > 10)
        if np.any(flags):
            message.append('Spotcheck split multiplier')
        else:
            message.append('*')
    except Exception as e:
        message.append('Splits: {}; '.format(e.__repr__()))

    bloomberg = divs.merge(spins, how='outer') \
        .merge(splits, how='outer').fillna(1)

    # Map SecCodes
    bloomberg = qad_map.merge(bloomberg)

    bloomberg = bloomberg[['SecCode', 'DivMultiplier',
                           'SpinoffMultiplier', 'SplitMultiplier']]

    # Write bloomberg data to file
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy',
                        'live_pricing', 'bloomberg_scaling.csv')
    bloomberg.to_csv(path, index=None)

    # Archived
    d = dt.date.today().strftime('%Y%m%d')
    file_name = '{}_bloomberg_scaling.csv'.format(d)
    path = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy',
                        'daily_data', file_name)
    bloomberg.to_csv(path, index=None)

    return message


def main():

    output = pd.DataFrame()

    message = process_bloomberg_data()

    output.loc[0, 'Desc'] = 'Bloomberg ID Mapping'
    output.loc[0, 'Message'] = message[0]

    output.loc[1, 'Desc'] = 'Bloomberg Dividends'
    output.loc[1, 'Message'] = message[1]

    output.loc[2, 'Desc'] = 'Bloomberg Spinoffs'
    output.loc[2, 'Message'] = message[2]

    output.loc[3, 'Desc'] = 'Bloomberg Splits'
    output.loc[3, 'Message'] = message[3]

    # OUTPUT to file
    dpath = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                         'StatArbStrategy', 'bloomberg_data_check.csv')
    output.to_csv(dpath, index=None)
    # Archive
    ddir = os.path.join(config.IMPLEMENTATION_DATA_DIR,
                        'StatArbStrategy', 'pretrade_check_archive')
    if not os.path.isdir(ddir):
        os.mkdir(ddir)

    prefix = get_todays_date_prefix()
    dpath = os.path.join(ddir, 'bloomberg_data_check_{}.csv'.format(prefix))
    output.to_csv(dpath, index=None)


if __name__ == '__main__':
    main()
