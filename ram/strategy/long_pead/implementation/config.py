
ids = {
    'sector20': [3892, 6027, 8156, 30902, 37953, 46692],
    'sector25': [3892, 6027, 8156, 30902, 37953, 46692],
    'sector45': [3892, 6027, 8156, 30902, 37953, 46692],
    'top800': [3892, 6027, 8156, 30902, 37953, 46692],
}


data_pull_features = [
    # Pricing
    'AdjClose', 'AdjVwap', 'RClose', 'RCashDividend', 'SplitFactor',
    'LAG1_AdjClose', 'LAG2_AdjClose', 'LAG3_AdjClose',

    # Descriptive
    'GGROUP', 'EARNINGSFLAG', 'MarketCap', 'AvgDolVol',

    # Pricing Features
    'PRMA120_AvgDolVol',
    'PRMA10_AdjClose', 'PRMA20_AdjClose',
    'BOLL10_AdjClose', 'BOLL20_AdjClose', 'BOLL60_AdjClose',
    'MFI10_AdjClose', 'MFI20_AdjClose', 'MFI60_AdjClose',
    'RSI10_AdjClose', 'RSI20_AdjClose', 'RSI60_AdjClose',
    'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
    'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
    'DISCOUNT252_AdjClose',

    # Accounting Features - No lag because new data points are
    # recorded at (T+1)
    'NETINCOMEQ', 'NETINCOMETTM',
    'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',

    'OPERATINGINCOMEQ', 'OPERATINGINCOMETTM',
    'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',

    'EBITQ', 'EBITTTM',
    'EBITGROWTHQ', 'EBITGROWTHTTM',

    'SALESQ', 'SALESTTM',
    'SALESGROWTHQ', 'SALESGROWTHTTM',

    'FREECASHFLOWQ', 'FREECASHFLOWTTM',
    'FREECASHFLOWGROWTHQ', 'FREECASHFLOWGROWTHTTM',

    'GROSSPROFASSET',
    'ASSETS',

    'GROSSMARGINTTM',
    'EBITDAMARGIN',

    'PE', 'FCFMARKETCAP', 'CASHEV',

    # StarMine - Lag because data for (T) isn't available until (T+1)
    'LAG1_ARM', 'LAG1_ARMREVENUE', 'LAG1_ARMRECS',
    'LAG1_ARMEARNINGS', 'LAG1_ARMEXRECS', 'LAG1_SIRANK',
    'LAG1_SIMARKETCAPRANK', 'LAG1_SISECTORRANK',
    'LAG1_SIUNADJRANK', 'LAG1_SISHORTSQUEEZE', 'LAG1_SIINSTOWNERSHIP',

    'PTARGETMEAN', 'PTARGETHIGH', 'PTARGETLOW', 'PTARGETUNADJ',
    'RECMEAN', 'RECHIGH', 'RECLOW', 'RECNREC'
]
