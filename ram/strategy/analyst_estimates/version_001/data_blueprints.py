from copy import deepcopy

from ram.data.data_constructor_blueprint import DataConstructorBlueprint

# BASE
base = DataConstructorBlueprint(constructor_type='universe',
                                description='TEMP',
                                market_data_flag=False,
                                strategy_name='AnalystEstimates')

base.universe_filter_arguments['filter'] = 'AvgDolVol'
base.universe_filter_arguments['where'] = 'MarketCap >= 200 and Close_ >= 10' \
                                            + ' and AvgDolVol >= 1'
base.universe_filter_arguments['univ_size'] = 1500

base.universe_date_parameters['frequency'] = 'Q'
base.universe_date_parameters['train_period_length'] = 1
base.universe_date_parameters['test_period_length'] = 1
base.universe_date_parameters['start_year'] = 2002


base.features = [
# Descriptive
'TM1', 'T1', 'GGROUP', 'EARNINGSRETURN', 'EARNINGSFLAG',
'MarketCap', 'AvgDolVol', 'SplitFactor', 'RVwap', 'RClose',
'RCashDividend',

# Pricing
'AdjOpen', 'AdjClose', 'AdjVwap', 'LEAD1_AdjVwap',
'LEAD20_AdjVwap', 'LEAD21_AdjVwap','LEAD22_AdjVwap',
'LEAD23_AdjVwap',

# Pricing Features
'PRMA10_AdjClose', 'PRMA20_AdjClose', 'PRMA60_AdjClose',
'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
'DISCOUNT252_AdjClose',

# Accounting Features
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

'ADJEPSQ', 'ADJEPSTTM',
'ADJEPSGROWTHQ', 'ADJEPSGROWTHTTM',

# Starmine Features
'EPSESTIMATEFQ1', 'EPSESTIMATEFQ2', 'EBITDAESTIMATEFQ1',
'EBITDAESTIMATEFQ2', 'REVENUEESTIMATEFQ1', 'REVENUEESTIMATEFQ2',

# Price Targets
'PTARGETMEAN', 'PTARGETHIGH', 'PTARGETLOW', 'PTARGETUNADJ',
'RECMEAN'
]


# All Sectors
univ1500_10 = deepcopy(base)
univ1500_10.description = 'Univ 1500 | Min $10'

univ1500_15 = deepcopy(base)
univ1500_15.universe_filter_arguments['where'] = 'MarketCap >= 200 and ' \
                                                    + 'Close_ >= 15 and ' \
                                                    + 'AvgDolVol >= 1'
univ1500_15.description = 'Univ 1500 | Min $15'
