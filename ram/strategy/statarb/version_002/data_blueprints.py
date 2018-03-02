from copy import deepcopy

from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer

# BASE
base = DataConstructorBlueprint(constructor_type='universe',
                                description='TEMP',
                                market_data_flag=True,
                                strategy_name='StatArbStrategy')

base.universe_filter_arguments['filter'] = 'AvgDolVol'
base.universe_filter_arguments['where'] = \
    'MarketCap >= 200 and Close_ between 5 and 500'
base.universe_filter_arguments['univ_size'] = 800

base.universe_date_parameters['frequency'] = 'Q'
base.universe_date_parameters['quarter_frequency_month_offset'] = 0
base.universe_date_parameters['train_period_length'] = 4
base.universe_date_parameters['test_period_length'] = 1
base.universe_date_parameters['start_year'] = 2001

base.market_data_params['features'] = ['AdjClose']

base.market_data_params['seccodes'] = [
    50311, 61258, 61259, 11097, 11099,
    11100, 10955, 11101, 11102, 11096,
    11103, 11104, 11113, 11132814, 10922530
]


base.features = [
    # Pricing
    'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'AdjVwap', 'AdjVolume',
    'RClose', 'RCashDividend', 'SplitFactor',

    'MarketCap', 'AvgDolVol',

    # Descriptive
    'GGROUP', 'EARNINGSFLAG',

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
    'ARM', 'ARMREVENUE', 'ARMRECS',
    'ARMEARNINGS', 'ARMEXRECS', 'SIRANK',
    'SIMARKETCAPRANK', 'SISECTORRANK',
    'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',

    'PTARGETMEAN', 'PTARGETHIGH', 'PTARGETLOW', 'PTARGETUNADJ',
    'RECMEAN', 'RECHIGH', 'RECLOW', 'RECNREC'
]


# SECTOR 20
sector20_0 = deepcopy(base)
sector20_0.description = 'Sector 20, Jan/Apr/Jul/Oct Version 001'
sector20_0.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_0.universe_date_parameters['quarter_frequency_month_offset'] = 0

sector20_1 = deepcopy(base)
sector20_1.description = 'Sector 20, Feb/May/Aug/Nov Version 001'
sector20_1.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_1.universe_date_parameters['quarter_frequency_month_offset'] = 1

sector20_2 = deepcopy(base)
sector20_2.description = 'Sector 20, Mar/Jun/Sep/Dec Version 001'
sector20_2.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_2.universe_date_parameters['quarter_frequency_month_offset'] = 2


# SECTOR 25
sector25_0 = deepcopy(base)
sector25_0.description = 'Sector 25, Jan/Apr/Jul/Oct Version 001'
sector25_0.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_0.universe_date_parameters['quarter_frequency_month_offset'] = 0

sector25_1 = deepcopy(base)
sector25_1.description = 'Sector 25, Feb/May/Aug/Nov Version 001'
sector25_1.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_1.universe_date_parameters['quarter_frequency_month_offset'] = 1

sector25_2 = deepcopy(base)
sector25_2.description = 'Sector 25, Mar/Jun/Sep/Dec Version 001'
sector25_2.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_2.universe_date_parameters['quarter_frequency_month_offset'] = 2


# SECTOR 45
sector45_0 = deepcopy(base)
sector45_0.description = 'Sector 45, Jan/Apr/Jul/Oct Version 001'
sector45_0.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_0.universe_date_parameters['quarter_frequency_month_offset'] = 0

sector45_1 = deepcopy(base)
sector45_1.description = 'Sector 45, Feb/May/Aug/Nov Version 001'
sector45_1.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_1.universe_date_parameters['quarter_frequency_month_offset'] = 1

sector45_2 = deepcopy(base)
sector45_2.description = 'Sector 45, Mar/Jun/Sep/Dec Version 001'
sector45_2.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_2.universe_date_parameters['quarter_frequency_month_offset'] = 2
