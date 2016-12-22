import re
import datetime as dt


#   All funcs need to be registered here so that they are properly
#   called in the parser

TECHFUNCS = ['MA', 'PRMA', 'VOL', 'BOLL', 'DISCOUNT', 'RSI', 'MFI']
MANIPFUNCS = ['LAG', 'LEAD', 'RANK']


def sqlcmd_from_feature_list(features, ids, start_date, end_date,
                             table='ram.dbo.ram_master_equities_research'):

    # Shallow copy
    features = features[:]

    if len(ids):
        ids_str = 'and SecCode in ' + format_ids(ids)
    else:
        ids_str = ''

    # First pull date ranges. Make this dynamic somehow?
    sdate = start_date - dt.timedelta(days=365)
    fdate = end_date + dt.timedelta(days=30)

    # Check if sector data is present
    gics_features = []
    if 'GSECTOR' in features:
        features.remove('GSECTOR')
        gics_features.append('GSECTOR')
    if 'GGROUP' in features:
        features.remove('GGROUP')
        gics_features.append('GGROUP')
    si_feature = False
    if 'SI' in features:
        features.remove('SI')
        si_feature = True
    ticker_feature = False
    if 'Ticker' in features:
        features.remove('Ticker')
        ticker_feature = True

    # Get individual entries for CTEs per feature
    ctes = []
    for f in features:
        ctes.append(make_cmds(f))
    # Format for entry into CTEs
    cte1, cte2, cte3, cte4 = zip(*ctes)
    vars1 = ', '.join(cte1)
    vars2 = ', '.join(cte2)
    vars3 = ', '.join(cte3)
    vars4 = ', '.join(cte4)

    sqlcmd = \
        """
        ; with cte1 as (
            select SecCode, Date_, {0}
            from {7}
            where Date_ between '{4}' and '{5}'
            {6}
        )
        , cte2 as (
            select SecCode, Date_, {1}
            from cte1
        )
        , cte3 as (
            select SecCode, Date_, {2}
            from cte2
        ), cte4 as (
            select SecCode, Date_, {3}
            from cte3
        )
        """.format(
            vars1, vars2, vars3, vars4,
            sdate, fdate, ids_str, table)

    # This string is used for the additional tables that are merged
    last_table = 'cte4'

    if gics_features:
        features += gics_features
        gics_features = ', '.join(gics_features)
        sqlcmd += \
            """
            , cte5 as (
                select A.*, {0} from {1} A
                left join ram.dbo.ram_sector B
                on A.SecCode = B.SecCode
                and A.Date_ between B.StartDate and B.EndDate
            )
            """.format(gics_features, last_table)
        last_table = 'cte5'

    if si_feature:
        features.append('SI')
        sqlcmd += \
            """
            , cte6 as (
                select A.*, B.SI as SI from {0} A
                join (select distinct IdcCode, SecCode
                      from {1}) C
                on A.SecCode = C.SecCode
                left join ram.dbo.ShortInterest B
                on C.IdcCode = B.IdcCode
                and B.Date_ = (
                    select max(Date_) from ram.dbo.ShortInterest b
                    where b.Date_ < A.Date_ and b.IdcCode = C.IdcCode)
            )
            """.format(last_table, table)
        last_table = 'cte6'

    if ticker_feature:
        features.append('Ticker')
        sqlcmd += \
            """
            , cte7 as (
                select A.*, B.HistoricalTicker as Ticker from {0} A
                left join {1} B
                on A.SecCode = B.SecCode
                and A.Date_ = B.Date_
            )
            """.format(last_table, table)
        last_table = 'cte7'

    sqlcmd += \
        """
        select * from {0}
        where Date_ between '{1}' and '{2}'
        order by SecCode, Date_
        """.format(last_table, start_date, end_date)
    return clean_sql_cmd(sqlcmd), features


def make_cmds(vstring):
    params = parse_input_var(vstring)
    # Call function that corresponds to user input. Will handle
    # if there is no manipulation for a variable, aka just return
    # data from the table.
    cte1, cte2, cte3 = globals()[params['var'][0]](params)
    cte4 = globals()[params['manip'][0]](params)
    return clean_sql_cmd(cte1), clean_sql_cmd(cte2), \
        clean_sql_cmd(cte3), clean_sql_cmd(cte4)


def parse_input_var(vstring):
    """
    Takes individual Feature and parses into dictionary used downstream.

    The format should be something like:
        LAG1_PRMA10_Close
    """
    args = [x for x in re.split('(\d+)|_', vstring) if x]
    out = {'name': '[{0}]'.format(vstring)}

    while args:

        # Manipulations
        if args[0] in MANIPFUNCS:
            out['manip'] = (args[0], int(args[1]))
            args = args[2:]

        # Variables
        elif args[0] in TECHFUNCS:
            out['var'] = (args[0], int(args[1]))
            args = args[2:]

        # Adjustment irrelevant columns
        elif args[0] in ['AvgDolVol', 'MarketCap', 'SplitFactor']:
            out['datacol'] = args[0]
            break

        # Adjusted data
        elif args[0] in ['Open', 'High', 'Low', 'Close', 'Vwap', 'Volume']:
            out['datacol'] = 'Adj' + args[0]
            break

        # Raw data
        elif args[0][0] == 'R':
            col = args[0][1:]
            assert col in ['Open', 'High', 'Low', 'Close', 'Vwap',
                           'Volume', 'CashDividend']
            if col in ['Open', 'Close']:
                col += '_'
            out['datacol'] = col
            break

        else:
            raise Exception('Input not properly formatted')

    if 'var' not in out:
        out['var'] = ('pass_through_var', 0)

    if 'manip' not in out:
        out['manip'] = ('pass_through_manip', 0)

    return out


def clean_sql_cmd(sqlcmd):
    return ' '.join(sqlcmd.replace('\n', ' ').split())


def format_ids(ids):
    """
    Takes in individual or list of ids, and returns a list/array
    of those ids, and a string used to query sql database.
    """
    if not hasattr(ids, '__iter__'):
        ids = [ids]
    idsstr = str([str(i) for i in ids])
    idsstr = idsstr.replace('[', '(').replace(']', ')')
    return idsstr


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def LAG(params):
    name = params['name']
    periods = params['manip'][1]

    sqlcmd = \
        """
        lag({0}, {1}) over (
            partition by SecCode
            order by Date_) as {0}
        """.format(name, periods)
    return sqlcmd


def LEAD(params):
    name = params['name']
    periods = params['manip'][1]

    sqlcmd = \
        """
        lead({0}, {1}) over (
            partition by SecCode
            order by Date_) as {0}
        """.format(name, periods)
    return sqlcmd


def RANK(params):
    name = params['name']
    sqlcmd = \
        """
        Rank() over (
            partition by Date_
            order by {0}) as {0}
        """.format(name)
    return sqlcmd


def pass_through_manip(params):
    name = params['name']
    sqlcmd = \
        """
        {0}
        """.format(name)
    return sqlcmd


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  All variable functions need to return TWO commands for CTEs

def pass_through_var(params):
    name = params['name']
    column = params['datacol']
    sqlcmd1 = \
        """
        {0} as {1}
        """.format(column, name)
    sqlcmd2 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2, sqlcmd2


def PRMA(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    sqlcmd1 = \
        """
        {0} / avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        """.format(column, length-1, name)
    sqlcmd2 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2, sqlcmd2


def MA(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    sqlcmd1 = \
        """
        avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        """.format(column, length-1, name)
    sqlcmd2 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2, sqlcmd2


def VOL(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    # Quick fix!
    name2 = name[1:-1]
    sqlcmd1 = \
        """
        {0} as {1},
        lag({0}, 1) over (
            partition by SecCode
            order by Date_) as TEMPLAG{2}
        """.format(column, name, name2)
    sqlcmd2 = \
        """
        stdev({1}/ TEMPLAG{2}) over (
            partition by SecCode
            order by Date_
            rows between {3} preceding and current row) as {1}
        """.format(column, name, name2, length-1)
    sqlcmd3 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2, sqlcmd3


def BOLL(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    # Quick fix!
    name2 = name[1:-1]
    sqlcmd1 = \
        """
        {0} as TEMPP{2},
        avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as TEMPMEAN{2},
        stdev({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as TEMPSTD{2}
        """.format(column, length-1, name2)
    sqlcmd2 = \
        """
        (TEMPP{0} - (TEMPMEAN{0} - 2 * TEMPSTD{0})) /
            nullif((4 * TEMPSTD{0}), 0) as {1}
        """.format(name2, name)
    sqlcmd3 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2, sqlcmd3


def DISCOUNT(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    sqlcmd1 = \
        """
        -1 * ({0} / max({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) - 1) as {2}
        """.format(column, length-1, name)
    sqlcmd2 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2, sqlcmd2


def RSI(params):
    length = params['var'][1]
    name = params['name']
    # Quick fix!
    name2 = name[1:-1]
    sqlcmd1 = \
        """
        case when
            (AdjClose - lag(AdjClose, 1) over (
                            partition by SecCode
                            order by Date_)) > 0
        then
            (AdjClose - lag(AdjClose, 1) over (
                            partition by SecCode
                            order by Date_))
        else 0 end as UpMove{0},

        case when
            (AdjClose - lag(AdjClose, 1) over (
                            partition by SecCode
                            order by Date_)) < 0
        then
            (AdjClose - lag(AdjClose, 1) over (
                            partition by SecCode
                            order by Date_))
        else 0 end as DownMove{0}
        """.format(name2)
    sqlcmd2 = \
        """
        sum(UpMove{0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as UpMove{0},

        sum(DownMove{0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as DownMove{0}
        """.format(name2, length-1)
    sqlcmd3 = \
        """
        100 * UpMove{0} / NullIf(UpMove{0} - DownMove{0}, 0) as {0}
        """.format(name2)
    return sqlcmd1, sqlcmd2, sqlcmd3


def MFI(params):
    length = params['var'][1]
    name = params['name']
    # Quick fix!
    name2 = name[1:-1]
    sqlcmd1 = \
        """
        (AdjHigh + AdjLow + AdjClose) / 3 as TypPrice{0},
        lag((AdjHigh + AdjLow + AdjClose) / 3, 1) over (
            partition by SecCode
            order by Date_) as LagTypPrice{0},
        (AdjHigh + AdjLow + AdjClose) / 3 * AdjVolume as RawMF{0}
        """.format(name2)

    sqlcmd2 = \
        """
        case
            when TypPrice{0} > LagTypPrice{0}
            then RawMF{0}
            else 0 end as MonFlowP{0},

        case
            when LagTypPrice{0} is null then RawMF{0}
            when TypPrice{0} != LagTypPrice{0} then RawMF{0}
            else 0 end as MonFlow{0}
        """.format(name2)

    sqlcmd3 = \
        """
        sum(MonFlowP{0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) /

        nullif(sum(MonFlow{0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row), 0) * 100 as {0}
        """.format(name2, length-1)
    return sqlcmd1, sqlcmd2, sqlcmd3
