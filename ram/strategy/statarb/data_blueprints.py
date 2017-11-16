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

base.features = [
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


# SECTOR 20
sector20_0 = deepcopy(base)
sector20_0.description = 'Sector 20, Jan/Apr/Jul/Oct'
sector20_0.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_0.universe_date_parameters['quarter_frequency_month_offset'] = 0

sector20_1 = deepcopy(base)
sector20_0.description = 'Sector 20, Feb/May/Aug/Nov'
sector20_1.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_1.universe_date_parameters['quarter_frequency_month_offset'] = 1

sector20_2 = deepcopy(base)
sector20_0.description = 'Sector 20, Mar/Jun/Sep/Dec'
sector20_2.universe_filter_arguments['where'] += ' and GSECTOR = 20'
sector20_2.universe_date_parameters['quarter_frequency_month_offset'] = 2


# SECTOR 25
sector25_0 = deepcopy(base)
sector20_0.description = 'Sector 25, Jan/Apr/Jul/Oct'
sector25_0.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_0.universe_date_parameters['quarter_frequency_month_offset'] = 0

sector25_1 = deepcopy(base)
sector20_0.description = 'Sector 25, Feb/May/Aug/Nov'
sector25_1.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_1.universe_date_parameters['quarter_frequency_month_offset'] = 1

sector25_2 = deepcopy(base)
sector20_0.description = 'Sector 25, Mar/Jun/Sep/Dec'
sector25_2.universe_filter_arguments['where'] += ' and GSECTOR = 25'
sector25_2.universe_date_parameters['quarter_frequency_month_offset'] = 2


# SECTOR 45
sector45_0 = deepcopy(base)
sector20_0.description = 'Sector 45, Jan/Apr/Jul/Oct'
sector45_0.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_0.universe_date_parameters['quarter_frequency_month_offset'] = 0

sector45_1 = deepcopy(base)
sector20_0.description = 'Sector 45, Feb/May/Aug/Nov'
sector45_1.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_1.universe_date_parameters['quarter_frequency_month_offset'] = 1

sector45_2 = deepcopy(base)
sector20_0.description = 'Sector 45, Mar/Jun/Sep/Dec'
sector45_2.universe_filter_arguments['where'] += ' and GSECTOR = 45'
sector45_2.universe_date_parameters['quarter_frequency_month_offset'] = 2


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

blueprint_container = DataConstructorBlueprintContainer()

blueprint_container.add_blueprint(sector20_0)
blueprint_container.add_blueprint(sector20_1)
blueprint_container.add_blueprint(sector20_2)

blueprint_container.add_blueprint(sector25_0)
blueprint_container.add_blueprint(sector25_1)
blueprint_container.add_blueprint(sector25_2)

blueprint_container.add_blueprint(sector45_0)
blueprint_container.add_blueprint(sector45_1)
blueprint_container.add_blueprint(sector45_2)
