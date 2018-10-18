from copy import deepcopy

from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer

# BASE
base = DataConstructorBlueprint(constructor_type='universe',
                                description='TEMP',
                                market_data_flag=False,
                                strategy_name='sandbox')


base.universe_filter_arguments['filter'] = 'AvgDolVol'

base.universe_filter_arguments['where'] = 'MarketCap >= 150 and Close_ >= 8' \
                                            + ' and AvgDolVol >= 1'

base.universe_filter_arguments['univ_size'] = 500

base.universe_date_parameters['frequency'] = 'Q'

base.universe_date_parameters['train_period_length'] = 1

base.universe_date_parameters['test_period_length'] = 1

base.universe_date_parameters['start_year'] = 2004


base.features = [
# Descriptive
'TM1', 'T1', 'GGROUP', 'MarketCap', 'AvgDolVol', 'SplitFactor',
'RVwap', 'RClose','RCashDividend',

# Pricing
'AdjOpen', 'LEAD1_AdjOpen', 'AdjClose', 'AdjVwap',  'LEAD1_AdjVwap',
'LEAD4_AdjVwap', 'LEAD5_AdjVwap', 'LEAD6_AdjVwap',
'LEAD7_AdjVwap', 'LEAD8_AdjVwap', 'LEAD9_AdjVwap', 'LEAD10_AdjVwap',
'LEAD11_AdjVwap', 'LEAD16_AdjVwap',

# Pricing Features
'PRMAH10_AdjClose', 'PRMAH20_AdjClose', 'PRMAH60_AdjClose',
'PRMAH120_AdjClose', 'PRMAH250_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose',
'VOL60_AdjClose', 'VOL120_AdjClose', 'VOL250_AdjClose',
'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',
'MFI10_AdjClose', 'MFI20_AdjClose', 'MFI60_AdjClose', 'MFI120_AdjClose',
'MFI250_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose', 'RSI60_AdjClose',
'RSI120_AdjClose', 'RSI250_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
'BOLL60_AdjClose', 'BOLL120_AdjClose', 'BOLL250_AdjClose',

# Starmine SI Vars
'SIRANK', 'SIMARKETCAPRANK', 'SISECTORRANK',
'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP',
# Starmine ARM
'ARM', 'ARMREVENUE', 'ARMRECS', 'ARMEARNINGS', 'ARMEXRECS',
# IBES Level 3
'PTARGETUNADJ', 'RECMEAN',

# Accounting
'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',
'SALESGROWTHQ', 'SALESGROWTHTTM',
'ADJEPSGROWTHQ', 'ADJEPSGROWTHTTM',
'GROSSMARGINTTM', 'PE'
]


# Make container
blueprint_container = DataConstructorBlueprintContainer()

for sector in range(10, 60, 5):
    sector_bp = deepcopy(base)
    sector_bp.description = 'Sector {}'.format(sector)
    sector_bp.universe_filter_arguments['where'] += ' and GSECTOR = {}'.format(sector)
    blueprint_container.add_blueprint(sector_bp)
