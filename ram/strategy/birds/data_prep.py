
from ram.data.data_constructor import DataConstructor

features = ['AvgDolVol', 'MarketCap', 'GSECTOR']
features += ['AdjClose', 'RClose', 'ROpen', 'RCashDividend', 'SplitFactor']
features += ['PRMA5_AvgDolVol', 'PRMA10_AvgDolVol', 'PRMA20_AvgDolVol']
features += ['PRMA5_AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose']
features += ['VOL5_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose']
features += ['BOLL5_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose']
features += ['DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
             'DISCOUNT252_AdjClose']
features += ['RSI5_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose']
features += ['MFI5_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose']
 
# Fundamentals that only really change with an earnings announcement
features += ['ACCTPRICESALES', 'ACCTEPSGROWTH', 'EARNINGSRETURN', 'EARNINGSFLAG']

dc = DataConstructor('BirdsStrategy')
dc.register_dates_parameters('M', 0, 2006)
dc.register_features(features)
dc.register_universe_size(200)

dc.run()
