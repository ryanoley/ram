import os
import sys
import numpy as np
import pandas as pd
from dateutil import parser as dp


ETB_DFLT = os.path.join(os.getenv('RAMSHARE'),
                        'Roundabout',
                        'Operations',
                        'Roundabout Accounting',
                        'RealTick EOD Files',
                        'Pershing_ETB')


def etb_status(tickers, ETB_DIR=ETB_DFLT):
    output_df = pd.DataFrame(data={'Ticker': list(tickers)})
    etb_data = get_max_etb_data(ETB_DIR)
    output_df['ETB_HTB'] = 'HTB'
    output_df.loc[output_df.Ticker.isin(etb_data.Ticker), 'ETB_HTB'] = 'ETB'
    return output_df


def get_max_etb_data(ETB_DIR=ETB_DFLT):
    """
    Gets and imports current ETB file
    """
    etb_fls = get_all_etb_files(ETB_DIR)
    if len(etb_fls) == 0:
        return pd.DataFrame(columns=['Ticker'])
    max_etb_fl_ix = np.argmax([dp.parse(x[:6]) for x in etb_fls])
    etbdata = pd.read_csv(os.path.join(ETB_DIR, etb_fls[max_etb_fl_ix]),
                          header=None, names=['Ticker'])
    return etbdata


def get_all_etb_files(ETB_DIR=ETB_DFLT):
    """
    Return all ETB files in the ETB directory
    """
    assert(os.path.exists(ETB_DIR))
    etb_fls = [x for x in os.listdir(ETB_DIR) if x.find('EZBR') >= 0]
    return etb_fls
