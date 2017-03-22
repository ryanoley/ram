
from ram.data.constructor import DataConstructor


features = ['AvgDolVol', 'MarketCap']

features += ['PRMA5_AvgDolVol', 'PRMA10_AvgDolVol', 'PRMA20_AvgDolVol']
features += ['PRMA5_AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose']

dc = DataConstructor('BirdsStrategy')
dc.register_dates_parameters('Q', 0, 2016)
dc.register_features(features)
dc.register_universe_size(10)
dc.run()


