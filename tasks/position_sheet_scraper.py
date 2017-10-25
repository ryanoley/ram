import os
import json
import numpy as np
import pandas as pd
import datetime as dt


def get_fund_manager_stat_arb_positions():
    base_path = os.getenv('DATA2')
    file_path = '/Common Folders/Roundabout/Operations/Roundabout Accounting/'
    file_path += 'Roundabout Daily P&L 2016.xlsx'
    path = base_path + file_path
    data = pd.read_excel(path)

    # Reset index on import doesn't work on server, so adding extra step
    indexes = data.index.to_frame().reset_index(drop=True)
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
    output_dir = os.path.join(os.getenv('DATA'), 'ram', 'position_sheet')
    data2.to_csv(os.path.join(output_dir, today+'_positions.csv'), index=None)

    with open(os.path.join(output_dir, today+'_portfolio.json'), 'w') as f:
        json.dump({'daily_pl': position_sheet_total_pl}, f)


if __name__ == '__main__':
    get_fund_manager_stat_arb_positions()
