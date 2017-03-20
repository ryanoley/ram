import re
import datetime as dt


###############################################################################

def sqlcmd_from_feature_list(features, ids, start_date, end_date,
                             table='ram.dbo.ram_equity_pricing_research'):
    feature_data = []
    for f in features:
        feature_data.append(
            parse_input_var(
                f, table,
                make_id_date_filter(ids, start_date, end_date)
            )
        )

    column_commands, join_commands = make_commands(feature_data)
    final_select_commands = make_final_commands(feature_data)
    filter_commands = make_id_date_filter(ids, start_date, end_date)

    # Combine everything
    sqlcmd = \
        """
        ; with X as (
        select A.SecCode, A.Date_ {0} from {1} A
        {2}
        {3})
        select {4} from X where Date_ between '{5}' and '{6}'
        """.format(column_commands, table, join_commands, filter_commands,
                   final_select_commands, start_date, end_date)

    return clean_sql_cmd(sqlcmd), features


def make_final_commands(feature_data):
    final_cmds = 'SecCode, Date_'
    for f in feature_data:
        if f['rank']:
            final_cmds += \
                """
                , RANK() over (
                    partition by Date_
                    order by {0}) as {0}
                """.format(f['feature_name'])
        else:
            final_cmds += \
                """
                , {0}
                """.format(f['feature_name'])
    return final_cmds


def make_commands(feature_data):

    col_cmds = ''
    join_cmds = ''

    for i, f in enumerate(feature_data):
        if f['shift']:
            shift_cmd, shift_n = f['shift']
            col_cmds += \
                """
                , {0}(x{1}.{2}, {3}) over (
                    partition by x{1}.SecCode
                    order by x{1}.Date_) as {2}
                """.format(shift_cmd, i, f['feature_name'], shift_n)
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


def make_id_date_filter(ids, start_date, end_date):
    # First pull date ranges. Make this dynamic somehow?
    sdate = start_date - dt.timedelta(days=365)
    fdate = end_date + dt.timedelta(days=30)
    sqlcmd = \
        """
        where A.Date_ between '{0}' and '{1}'
        and A.SecCode in {2}
        """.format(sdate, fdate, format_ids(ids))
    return sqlcmd


###############################################################################

def parse_input_var(vstring, table, filter_commands):

    FUNCS = ['MA', 'PRMA', 'VOL', 'BOLL', 'DISCOUNT', 'RSI', 'MFI',
             'GSECTOR', 'GGROUP', 'SI', 'PRMAH', 'EARNINGSFLAG',
             'ACCTSALES', 'ACCTSALESGROWTH', 'ACCTSALESGROWTHTTM',
             'ACCTEPSGROWTH', 'ACCTPRICESALES']

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

        elif arg[0] in FUNCS:
            sql_func = globals()[arg[0]]
            try:
                sql_func_args = int(arg[1])
            except:
                sql_func_args = None

        # Raw data
        elif arg[0] in ['ROpen', 'RHigh', 'RLow', 'RClose', 'RVwap',
                        'RVolume', 'RCashDividend']:
            arg[0] = arg[0][1:]
            sql_func_data_column = arg[0]
            if arg[0] in ['Open', 'Close']:
                sql_func_data_column += '_'

        # Data to be passed to a technical function
        elif (sql_func != DATACOL) and \
             (arg[0] in ['Open', 'High', 'Low', 'Close', 'Vwap', 'Volume']):
            sql_func_data_column = 'Adj' + arg[0]

        # Adjusted data
        elif arg[0] in ['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose',
                        'AdjVwap', 'AdjVolume']:
            sql_func_data_column = arg[0]

        # Adjustment irrelevant columns
        elif arg[0] in ['AvgDolVol', 'MarketCap', 'SplitFactor',
                        'HistoricalTicker']:
            sql_func_data_column = arg[0]

        else:
            raise Exception('Input not properly formatted: {{ %s }}' % vstring)

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
        from {2} A
        """.format(data_column, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def MA(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select SecCode, Date_, avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        from {3} A
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def PRMA(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select SecCode, Date_, {0} / avg({0}) over (
            partition by SecCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        from {3} A
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def PRMAH(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select  A.SecCode,
                A.Date_,
                A.{0} / avg(A.{0}) over (
                    partition by A.SecCode
                    order by A.Date_
                    rows between {1} preceding and current row) -
                    B.PRMASPY as {2}
        from    {3} A
        join (
            select  b.Date_,
                    b.{0} / avg(b.{0}) over (
                        order by b.Date_
                        rows between {1} preceding and current row) as PRMASPY
            from    ram.dbo.ram_master_etf b
            where   B.SecCode = 61494
            ) B
            on  A.Date_ = B.Date_
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def MFI(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select SecCode, Date_,
            sum(MonFlowP) over (
                partition by SecCode
                order by Date_
                rows between {0} preceding and current row) /

            nullif(sum(MonFlow) over (
                partition by SecCode
                order by Date_
                rows between {0} preceding and current row), 0) * 100 as {1}

            from

        (
        select SecCode, Date_,
            case
                when TypPrice > LagTypPrice
                then RawMF
                else 0 end as MonFlowP,

            case
                when LagTypPrice is null then RawMF
                when TypPrice != LagTypPrice then RawMF
                else 0 end as MonFlow

            from
        (
        select SecCode, Date_,
            (AdjHigh + AdjLow + AdjClose) / 3 as TypPrice,

            lag((AdjHigh + AdjLow + AdjClose) / 3, 1) over (
                partition by SecCode
                order by Date_) as LagTypPrice,
            (AdjHigh + AdjLow + AdjClose) / 3 * AdjVolume as RawMF
            from {2}
        ) a ) A
        """.format(length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def RSI(data_column, feature_name, length_arg, table):
    sqlcmd = \
        """
        select SecCode, Date_,
            100 * UpMove / NullIf(UpMove - DownMove, 0) as {0}
        from
        (
        select SecCode, Date_,
            sum(UpMove) over (
                partition by SecCode
                order by Date_
                rows between {1} preceding and current row) as UpMove,

            sum(DownMove) over (
                partition by SecCode
                order by Date_
                rows between {1} preceding and current row) as DownMove
        from
        (
        select SecCode, Date_,
            case when
                (AdjClose - lag(AdjClose, 1) over (
                                partition by SecCode
                                order by Date_)) > 0
            then
                (AdjClose - lag(AdjClose, 1) over (
                                partition by SecCode
                                order by Date_))
            else 0 end as UpMove,

            case when
                (AdjClose - lag(AdjClose, 1) over (
                                partition by SecCode
                                order by Date_)) < 0
            then
                (AdjClose - lag(AdjClose, 1) over (
                                partition by SecCode
                                order by Date_))
            else 0 end as DownMove
            from {2}
        ) a ) A
    """.format(feature_name, length_arg-1, table)
    return clean_sql_cmd(sqlcmd)


def DISCOUNT(data_column, feature_name, length_arg, table):
    if data_column is None:
        assert "DISCOUNT requires data column"
    sqlcmd = \
        """
        select SecCode, Date_,
            -1 * ({0} / max({0}) over (
                partition by SecCode
                order by Date_
                rows between {1} preceding and current row) - 1) as {2}
        from {3} A
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def BOLL(data_column, feature_name, length_arg, table):
    if data_column is None:
        assert "BOLL requires data column"
    sqlcmd = \
        """
        select SecCode, Date_,
            (Price - (AvgPrice - 2 * StdPrice)) / nullif((4 * StdPrice), 0)
            as {0}
        from
        (
            select SecCode, Date_,
                {1} as Price,
                avg({1}) over (
                    partition by SecCode
                    order by Date_
                    rows between {2} preceding and current row) as AvgPrice,
                stdev({1}) over (
                    partition by SecCode
                    order by Date_
                    rows between {2} preceding and current row) as StdPrice
            from {3}
        ) A
        """.format(feature_name, data_column, length_arg-1, table)
    return clean_sql_cmd(sqlcmd)


def VOL(data_column, feature_name, length_arg, table):
    if data_column is None:
        assert "VOL requires data column"
    sqlcmd = \
        """
        select SecCode, Date_,
            stdev(Price/ LagPrice) over (
                partition by SecCode
                order by Date_
                rows between {0} preceding and current row) as {1}
        from
        (
            select SecCode, Date_,
                {2} as Price,
                lag({2}, 1) over (
                    partition by SecCode
                    order by Date_) as LagPrice
            from {3}
        ) A
        """.format(length_arg-1, feature_name, data_column, table)
    return clean_sql_cmd(sqlcmd)


def GSECTOR(arg0, arg1, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.GSECTOR
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_sector B
            on      G.GVKey = B.GVKey
            and     A.Date_ between B.StartDate and B.EndDate
        """.format(table)
    return clean_sql_cmd(sqlcmd)


def GGROUP(arg0, arg1, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.GGROUP
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_sector B
            on      G.GVKey = B.GVKey
            and     A.Date_ between B.StartDate and B.EndDate
        """.format(table)
    return clean_sql_cmd(sqlcmd)


def EARNINGSFLAG(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    isnull(B.EarningsFlag, 0) as {1}
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   (select GVKey, ReportDate, 1 as EarningsFlag
                     from ram.dbo.ram_equity_report_dates) B
            on      G.GVKey = B.GVKey
            and     A.Date_ = B.ReportDate
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def SI(arg0, arg1, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.SI
        from        {0} A
        left join   ram.dbo.ShortInterest B
            on      A.IdcCode = B.IdcCode
            and     B.Date_ = (
                select max(Date_) from ram.dbo.ShortInterest b
                where b.Date_ <= B.Date_ and b.IdcCode = B.IdcCode
            )
        """.format(table)
    return clean_sql_cmd(sqlcmd)


def ACCTSALES(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.Value_ as {1}
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting B
            on      G.GVKey = B.GVKey
            and     B.Item = 'SALEQ'
            and     B.ReportDate = (select max(ReportDate)
                        from ram.dbo.ram_compustat_accounting
                        where GVKey = G.GVKey and ReportDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def ACCTSALESGROWTH(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.ValueGrowth as {1}
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting B
            on      G.GVKey = B.GVKey
            and     B.Item = 'SALEQ'
            and     B.ReportDate = (select max(ReportDate)
                        from ram.dbo.ram_compustat_accounting
                        where GVKey = G.GVKey and ReportDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def ACCTSALESGROWTHTTM(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.ValueGrowthTTM as {1}
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting B
            on      G.GVKey = B.GVKey
            and     B.Item = 'SALEQ'
            and     B.ReportDate = (select max(ReportDate)
                        from ram.dbo.ram_compustat_accounting
                        where GVKey = G.GVKey and ReportDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def ACCTEPSGROWTH(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.ValueGrowth as {1}
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting B
            on      G.GVKey = B.GVKey
            and     B.Item = 'EPSFXQ'
            and     B.ReportDate = (select max(ReportDate)
                        from ram.dbo.ram_compustat_accounting
                        where GVKey = G.GVKey and ReportDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def ACCTPRICESALES(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    A.MarketCap / B.Value_ as {1}
        from        {0} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting B
            on      G.GVKey = B.GVKey
            and     B.Item = 'SALEQ'
            and     B.ReportDate = (select max(ReportDate)
                        from ram.dbo.ram_compustat_accounting
                        where GVKey = G.GVKey and ReportDate < A.Date_)
        """.format(table, feature_name)
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
