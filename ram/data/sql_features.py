import re
import datetime as dt


def sqlcmd_from_feature_list(features, ids, start_date, end_date):

    if len(ids):
        ids_str = 'and IdcCode in ' + format_ids(ids)
    else:
        ids_str = ''

    # Make this dynamic somehow?
    sdate = start_date - dt.timedelta(days=365)

    # Get individual entries for CTEs per feature
    ctes = []
    for f in features:
        ctes.append(make_cmds(f))
    # Format for entry into CTEs
    cte1, cte2, cte3 = zip(*ctes)
    vars1 = ', '.join(cte1)
    vars2 = ', '.join(cte2)
    vars3 = ', '.join(cte3)

    sqlcmd = \
        """
        ; with cte1 as (
            select IdcCode, Date_, {0}
            from ram.dbo.ram_master_equities
            where Date_ between '{3}' and '{5}'
            {6}
        )
        , cte2 as (
            select IdcCode, Date_, {1}
            from cte1
        )
        , cte3 as (
            select IdcCode, Date_, {2}
            from cte2
        )
        select * from cte3
        where Date_ between '{4}' and '{5}'
        """.format(
            vars1, vars2, vars3,
            sdate, start_date, end_date, ids_str)
    return clean_sql_cmd(sqlcmd)


def make_cmds(vstring):
    params = parse_input_var(vstring)
    # Call function that corresponds to user input. Will handle
    # if there is no manipulation for a variable, aka just return
    # data from the table.
    cte1, cte2 = globals()[params['var'][0]](params)
    cte3 = globals()[params['manip'][0]](params)
    return clean_sql_cmd(cte1), clean_sql_cmd(cte2), clean_sql_cmd(cte3)


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
        if args[0] in ['LAG', 'LEAD', 'RANK']:
            out['manip'] = (args[0], int(args[1]))
            args = args[2:]

        # Variables
        elif args[0] in ['MA', 'PRMA', 'VOL', 'BOLL']:
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
            partition by IdcCode
            order by Date_) as {0}
        """.format(name, periods)
    return sqlcmd


def LEAD(params):
    name = params['name']
    periods = params['manip'][1]

    sqlcmd = \
        """
        lead({0}, {1}) over (
            partition by IdcCode
            order by Date_) as {0}
        """.format(name, periods)
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
    return sqlcmd1, sqlcmd2


def PRMA(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    sqlcmd1 = \
        """
        {0} / avg({0}) over (
            partition by IdcCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        """.format(column, length-1, name)
    sqlcmd2 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2


def MA(params):
    column = params['datacol']
    length = params['var'][1]
    name = params['name']
    sqlcmd1 = \
        """
        avg({0}) over (
            partition by IdcCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        """.format(column, length-1, name)
    sqlcmd2 = \
        """
        {0}
        """.format(name)
    return sqlcmd1, sqlcmd2


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
            partition by IdcCode
            order by Date_) as TEMPLAG{2}
        """.format(column, name, name2)
    sqlcmd2 = \
        """
        stdev({1}/ TEMPLAG{2}) over (
            partition by IdcCode
            order by Date_
            rows between {3} preceding and current row) as {1}
        """.format(column, name, name2, length-1)
    return sqlcmd1, sqlcmd2


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
            partition by IdcCode
            order by Date_
            rows between {1} preceding and current row) as TEMPMEAN{2},
        stdev({0}) over (
            partition by IdcCode
            order by Date_
            rows between {1} preceding and current row) as TEMPSTD{2}
        """.format(column, length-1, name2)
    sqlcmd2 = \
        """
        (TEMPP{0} - (TEMPMEAN{0} - 2 * TEMPSTD{0})) / (4 * TEMPSTD{0}) as {1}
        """.format(name2, name)
    return sqlcmd1, sqlcmd2
