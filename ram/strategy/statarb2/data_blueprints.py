from copy import deepcopy

from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer


# BASE
base = DataConstructorBlueprint(constructor_type='universe',
                                description='TEMP',
                                market_data_flag=True,
                                strategy_name='StatArbStrategy2')

# Filter arguments
base.universe_filter_arguments['filter'] = 'AvgDolVol'

base.universe_filter_arguments['where'] = \
    'MarketCap >= 200 and Close_ between 5 and 500'

base.universe_filter_arguments['univ_size'] = 800

# Date arguments
base.universe_date_parameters['frequency'] = 'M'

base.universe_date_parameters['train_period_length'] = 3

base.universe_date_parameters['test_period_length'] = 2

base.universe_date_parameters['start_year'] = 2004

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

    # IBES
    'PTARGETMEAN', 'PTARGETHIGH', 'PTARGETLOW', 'PTARGETUNADJ',
    'RECMEAN', 'RECHIGH', 'RECLOW', 'RECNREC'
]


# Make container
blueprint_container = DataConstructorBlueprintContainer()

for sector in range(10, 60, 5):
    sector_bp = deepcopy(base)
    sector_bp.description = 'Sector {}'.format(sector)
    sector_bp.universe_filter_arguments['where'] += ' and GSECTOR = {}'.format(sector)
    blueprint_container.add_blueprint(sector_bp)
