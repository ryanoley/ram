"""
~~~~~~~~ CAM1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Designed to capture information from options prices and volumes
* Date doesn't need to be lagged given information delivered before open
* CAM1 is main index score

Constituent parts:

    * Spread: 
        Deviations from put/call parity, ie stocks with calls that are
        relatively expensive to puts tend to go up. Spread is calculated by
        comparing the implied volatilities of puts and calls with matched
        moneyness and expirations.

    * Skew:
        Steep skews represent options traders expressing negative
        views as out of the money options are relatilvely cheap and pay
        out on large drops. difference in implied volatilities between an
        out-of-the-money put and an at-the-money call for same expirations.

    * Volume:
        Compares call vs put volumes. In the presence of short sale constraint
        there should be a big difference in puts vs calls.


~~~~~~~~ Digital Revenue ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Leverages real-time commercially available online consumer behavior information
across multiple websites, Search and social platforms. Bottom-ranked
stocks tended not to beat their expectations where top-ranked stocks did.


~~~~~~~~ TM1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Designed to capture the technical dynamics of single US equities over one to ten
trading day horizons. Expands on traditional reversal factors by identifying
which ones are actually likely to reverse, and incorporating liquidity
and seasonality effects.

Should be considered an upgrade to traditional reversal factors.


~~~~~~ TRESS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Captures whether top financial bloggers have provided Buy or Sell recommendations
using proporietary NLP, that takes into account recency and quality.

"""
import os
import pandas as pd
from ram.utils.time_funcs import convert_date_array


def get_extract_alpha_data(data, features, ram_data):
    """
    Parameters
    ----------
    data : pd.DataFrame
        This is the Extract Alpha data file
    ram_data : pd.DataFrame
        This is the ram data file that data will be merged with
    features : list
        List of features from the `data` file that should be returned
    """
    ram_data = ram_data[['SecCode', 'Date', 'TICKER', 'CUSIP']].copy()
    # First merge on Cusip
    ram_data2 = ram_data.merge(data[['Date', 'CUSIP'] + features], how='left')
    # Get matched/unmatched SecCodes
    temp = ram_data2.copy()
    temp[features] = temp[features].isnull().astype(int)
    # Group by is to cound proportion that are missing. Second mean
    # calculation is to identify SecCodes that have no matches for any
    # feature
    temp = temp.groupby('SecCode')[features].mean().mean(axis=1) == 1
    unmatched_seccodes = temp[temp].index.values
    matched_seccodes = temp[~temp].index.values
    # Separate matched from unmatched
    ram_data3 = ram_data2[ram_data2.SecCode.isin(unmatched_seccodes)].copy()
    ram_data2 = ram_data2[ram_data2.SecCode.isin(matched_seccodes)].copy()
    # Drop feature columns and remerge on ticker
    ram_data3 = ram_data3.drop(features, axis=1)
    ram_data3 = ram_data3.merge(data[['Date', 'TICKER'] + features], how='left')

    # Something to do with these?
    #missing_indexes = ram_data3[features].isnull().all(axis=1)
    #ram_data3[missing_indexes]['SecCode'].unique()

    return ram_data2.append(ram_data3).reset_index(drop=True)


def _format_clean_data(data, features):
    data = data.rename(columns={'date': 'Date',
                                'cusip': 'CUSIP',
                                'Cusip': 'CUSIP',
                                'ticker': 'TICKER',
                                'Ticker': 'TICKER'})
    data.CUSIP = data.CUSIP.apply(lambda x: str(x)[:8])
    data.Date = convert_date_array(data.Date)

    data = data[['Date', 'CUSIP', 'TICKER'] + features]
    return data


def read_all_extract_alpha_files():
    ddir = os.path.join(os.getenv('DATA'), 'extractalpha')

    # ~~~ CAM1 ~~~
    features1 = ['spread_component', 'skew_component', 'volume_component',
                 'CAM1', 'CAM1_slow']
    dpath = os.path.join(ddir, 'CAM1_History_2005_201707.csv')
    data1 = _format_clean_data(pd.read_csv(dpath), features1)

    # ~~~ Digital Revenue ~~~
    features2 = ['Digital_Revenue_Signal']
    dpath = os.path.join(ddir, 'Digital_Revenue_Signal_history_2012_201707.csv')
    data2 = _format_clean_data(pd.read_csv(dpath), features2)

    # ~~~ TM ~~~
    features3 = ['reversal_component', 'factor_momentum_component',
                 'liquidity_shock_component', 'seasonality_component', 'tm1']
    dpath = os.path.join(ddir, 'TM1_History_2000_201701.csv')
    data3 = _format_clean_data(pd.read_csv(dpath), features3)

    # ~~~ Tress ~~~
    features4 = ['TRESS']
    dpath = os.path.join(ddir, 'TRESS_history_2010_201707.csv')
    data4 = _format_clean_data(pd.read_csv(dpath), features4)

    return {
        'cam1': (data1, features1),
        'dr': (data2, features2),
        'tm': (data3, features3),
        'tress': (data4, features4)
    }
