import re
import datetime as dt


###############################################################################

def sqlcmd_from_feature_list(features, ids, start_date, end_date,
                             table='ram.dbo.ram_master_equities_research'):
    feature_data = []
    for f in features:
        feature_data.append(
            parse_input_var(
                f, table,
                make_id_date_filter(ids, start_date, end_date)
            )
        )

    column_commands, join_commands = make_commands(feature_data)
    filter_commands = make_id_date_filter(ids, start_date, end_date, 'A.')

    # Combine everything
    sqlcmd = \
        """
        select A.SecCode, A.Date_ {0} from {1} A
        {2}
        {3}
        """.format(column_commands, table, join_commands, filter_commands)

    return clean_sql_cmd(sqlcmd), features


def make_commands(feature_data):

    col_cmds = ''
    join_cmds = ''

    for i, f in enumerate(feature_data):

        if f['shift'] and f['rank']:
            shift_cmd, shift_n = f['shift']
            col_cmds += \
                """
                , RANK({0}(x{1}.{2}, {3}) over (
                    partition by SecCode
                    order by Date_)) over (
                    partition by SecCode
                    order by Date_) as {2}
                """.format(shift_cmd, i, f['feature_name'], shift_n)

        elif f['shift']:
            shift_cmd, shift_n = f['shift']
            col_cmds += \
                """
                , {0}(x{1}.{2}, {3}) over (
                    partition by SecCode
                    order by Date_) as {2}
                """.format(shift_cmd, i, f['feature_name'], shift_n)

        elif f['rank']:
            col_cmds += \
                """
                , RANK(x{0}.{1}) over (
                    partition by SecCode
                    order by Date_) as {1}
                """.format(i, f['feature_name'])

        else:
            col_cmds += \
                """
                , x{0}.{1}
                """.format(i, f['feature_name'])

        join_cmds += \
            """
            left join ({0}) x{1}
                on A.SecCode = x{1}.SecCode
                and A.Date_ = x{1}.Date_
            """.format(f['sqlcmd'], i)

    return clean_sql_cmd(col_cmds), clean_sql_cmd(join_cmds)


def make_id_date_filter(ids, start_date, end_date, prefix=''):
    # First pull date ranges. Make this dynamic somehow?
    sdate = start_date - dt.timedelta(days=365)
    fdate = end_date + dt.timedelta(days=30)
    sqlcmd = \
        """
        where {0}Date_ between '{1}' and '{2}'
        and {0}SecCode in {3}
        """.format(prefix, sdate, fdate, format_ids(ids))
    return sqlcmd


###############################################################################

def parse_input_var(vstring, table, filter_commands):

    TECHFUNCS = ['MA', 'PRMA', 'VOL', 'BOLL', 'DISCOUNT', 'RSI', 'MFI']

    # Return object that is used downstream per requested feature
    out = {
        'shift': False,
        'rank': False,
        'feature_name': vstring,
        'sqlcmd': False
    }

    # Function used to generate SQL script
    sql_func = DATACOL
    sql_func_args = None
    sql_func_data_column = None

    # Parse and iterate input args
    for arg in vstring.split('_'):

        arg = re.findall('\d+|\D+', arg)

        if arg[0] in ['LEAD', 'LAG']:
            out['shift'] = (arg[0], int(arg[1]))

        elif arg[0] == 'RANK':
            out['rank'] = True

        elif arg[0] in TECHFUNCS:
            sql_func = globals()[arg[0]]
            sql_func_args = int(arg[1])

        # Adjusted data
        elif arg[0] in ['Open', 'High', 'Low', 'Close', 'Vwap', 'Volume']:
            sql_func_data_column = 'Adj' + arg[0]

        # Raw data
        elif arg[0] in ['ROpen', 'RHigh', 'RLow', 'RClose',
                        'RVwap', 'RVolume', 'CRashDividend']:
            sql_func_data_column = arg[0][1:]
            if sql_func_data_column in ['Open', 'Close']:
                sql_func_data_column += '_'

        # Adjustment irrelevant columns
        elif arg[0] in ['AvgDolVol', 'MarketCap', 'SplitFactor']:
            sql_func_data_column = arg[0]

        else:
            raise Exception('Input not properly formatted')

    out['sqlcmd'] = sql_func(sql_func_data_column, vstring,
                             sql_func_args, table)
    out['sqlcmd'] += filter_commands

    return out


###############################################################################
#  NOTE: All feature functions must have the same interface

def DATACOL(data_column, feature_name, args, table):
    sqlcmd = \
        """
        select SecCode, Date_, {0} as {1}
        from {2}
        """.format(data_column, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def MA(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select SecCode, Date_, avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        from {3}
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def PRMA(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select SecCode, Date_, {0} / avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        from {3}
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


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
