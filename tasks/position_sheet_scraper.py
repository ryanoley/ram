import os
import json
import shutil
import numpy as np
import pandas as pd
import datetime as dt

DATA = os.getenv('DATA')
RAM_ACCNT = os.path.join(os.getenv('RAMSHARE'),
                         'Roundabout',
                         'Operations',
                         'Roundabout Accounting')

FM_EXCEL_PATH = os.path.join(RAM_ACCNT,
                             'Roundabout Daily P&L 2016.xlsx')
FM_EXPORT_PATH = os.path.join(RAM_ACCNT,
                              'Fund Manager 2016',
                              'Fund Manager Export 2016.csv')


def get_fund_manager_stat_arb_positions():
    data = pd.read_excel(FM_EXCEL_PATH)

    # Reset index on import doesn't work on server, so adding extra step
    indexes = pd.DataFrame(data.index.values.tolist())
    indexes.columns = ['col%s' % s for s in range(indexes.shape[1])]
    data = data.reset_index(drop=True)
    data = data.join(indexes)

    # Identify correct column numbers for Position, Symbol, Share Count
    for i, d in data.iterrows():
        if 'Position' in d.values:
            break

    column_inds = [
        np.where(d.values == 'Position')[0][0],
        np.where(d.values == 'Symbol')[0][0],
        np.where(d.values == 'Share Count')[0][0],
        np.where(d.values == 'Market Price (USD)')[0][0],
        np.where(d.values == 'Position Value')[0][0],
        np.where(d.values == 'Daily P&L')[0][0],
        min(np.where(d.values == '% of AUM')[0]),
    ]

    data2 = data.iloc[(i+1):, column_inds].reset_index(drop=True)
    data2.columns = ['position', 'symbol', 'share_count', 'market_price',
                     'position_value', 'daily_pl', 'position_value_perc_aum']

    # Cut off portfolio information
    for i, d in data2.iterrows():
        if 'TOTAL DAILY P&L (gross)' in d.values:
            break
    position_sheet_total_pl = d.symbol
    data2 = data2.loc[:(i-1)]

    # Clean up data
    data2 = data2[data2.symbol.notnull()]
    data2 = data2[data2.share_count.apply(np.isreal)]
    data2 = data2[(data2.share_count != 0) | (data2.daily_pl != 0)]

    # Write data to file
    today = dt.datetime.now().strftime('%Y%m%d')
    output_dir = os.path.join(DATA, 'ram', 'position_sheet')
    data2.to_csv(os.path.join(output_dir, today+'_positions.csv'), index=None)

    with open(os.path.join(output_dir, today+'_portfolio.json'), 'w') as f:
        json.dump({'daily_pl': position_sheet_total_pl}, f)


def archive_fund_manager_export():
    # Add file creation datetime
    dt_mod = dt.datetime.fromtimestamp(os.path.getmtime(FM_EXPORT_PATH))

    # Write data to file
    datestamp = dt_mod.strftime('%Y%m%d')
    output_dir = os.path.join(DATA, 'ram', 'position_sheet', 'archive')
    output_path = os.path.join(output_dir,
                               '{}_fm_export.csv'.format(datestamp))

    shutil.copy2(FM_EXPORT_PATH, output_path)


if __name__ == '__main__':
    get_fund_manager_stat_arb_positions()
    archive_fund_manager_export()
