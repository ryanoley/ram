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

base.universe_date_parameters['frequency'] = 'M'

base.universe_date_parameters['train_period_length'] = 3

base.universe_date_parameters['test_period_length'] = 2

base.universe_date_parameters['start_year'] = 2001

base.market_data_params['features'] = ['AdjClose']

"""
# Market arguments
* 50311: SP500 Index
* 11113: VIX
* 11097: R1000Index
* 11099: R1000Growth
* 11100: R1000Value
"""
base.market_data_params['features'] = ['AdjClose']

base.market_data_params['seccodes'] = [
    50311, 11113, 11097, 11099, 111000
]


# Features
base.features = [
    # Pricing
    'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'AdjVwap', 'AdjVolume',
    'RClose', 'RCashDividend', 'SplitFactor',
    'MarketCap', 'AvgDolVol',

    # Descriptive
    'GGROUP', 'EARNINGSFLAG',

    # Accounting Features
    'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',
    'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',
    'EBITGROWTHQ', 'EBITGROWTHTTM',
    'SALESGROWTHQ', 'SALESGROWTHTTM',
    'FREECASHFLOWGROWTHQ', 'FREECASHFLOWGROWTHTTM',
    'PE',

    # StarMine - Lag because data for (T) isn't available until (T+1)
    'ARM', 'ARMREVENUE', 'ARMRECS',
    'ARMEARNINGS', 'ARMEXRECS', 'SIRANK',
    'SIMARKETCAPRANK', 'SISECTORRANK',
    'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',

]


# SECTOR 20
sector20_1 = deepcopy(base)
sector20_1.description = 'Sector 20, Monthly, 12 month training'
sector20_1.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 45
sector25_1 = deepcopy(base)
sector25_1.description = 'Sector 25, Monthly, 12 month training'
sector25_1.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 45
sector45_1 = deepcopy(base)
sector45_1.description = 'Sector 45, Monthly, 12 month training'
sector45_1.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_1.universe_date_parameters['train_period_length'] = 12


####
# SECTOR 10
sector10_1 = deepcopy(base)
sector10_1.description = 'Sector 10, Monthly, 12 month training'
sector10_1.universe_filter_arguments['where'] += ' and GSECTOR = 10'
sector10_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 15
sector15_1 = deepcopy(base)
sector15_1.description = 'Sector 15, Monthly, 12 month training'
sector15_1.universe_filter_arguments['where'] += ' and GSECTOR = 15'
sector15_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 30
sector30_1 = deepcopy(base)
sector30_1.description = 'Sector 30, Monthly, 12 month training'
sector30_1.universe_filter_arguments['where'] += ' and GSECTOR = 30'
sector30_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 35
sector35_1 = deepcopy(base)
sector35_1.description = 'Sector 35, Monthly, 12 month training'
sector35_1.universe_filter_arguments['where'] += ' and GSECTOR = 35'
sector35_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 40
sector40_1 = deepcopy(base)
sector40_1.description = 'Sector 40, Monthly, 12 month training'
sector40_1.universe_filter_arguments['where'] += ' and GSECTOR = 40'
sector40_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 50
sector50_1 = deepcopy(base)
sector50_1.description = 'Sector 50, Monthly, 12 month training'
sector50_1.universe_filter_arguments['where'] += ' and GSECTOR = 50'
sector50_1.universe_date_parameters['train_period_length'] = 12

# SECTOR 55
sector55_1 = deepcopy(base)
sector55_1.description = 'Sector 55, Monthly, 12 month training'
sector55_1.universe_filter_arguments['where'] += ' and GSECTOR = 55'
sector55_1.universe_date_parameters['train_period_length'] = 12
