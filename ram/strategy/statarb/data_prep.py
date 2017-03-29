
from ram.data.data_constructor import DataConstructor

features = ['AdjClose', 'AvgDolVol',
            'RClose', 'RCashDividend', 'SplitFactor',
            'GSECTOR', 'EARNINGSFLAG']

dc = DataConstructor('StatArbStrategy')
dc.register_dates_parameters('M', 13, 2003)
dc.register_features(features)
dc.register_universe_size(500)

dc.run()
