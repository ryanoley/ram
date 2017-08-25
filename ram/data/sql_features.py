import re
import datetime as dt


# FUNCTIONS/Column heads that are registered

FUNCS = [
    # Pricing Varabies
    'MA', 'PRMA', 'VOL', 'BOLL', 'DISCOUNT', 'MIN', 'MAX', 'RSI',
    'MFI', 'SI', 'PRMAH', 'EARNINGSFLAG',
    'EARNINGSRETURN', 'MKT',

    # QUALITATIVE
    'GSECTOR', 'GGROUP', 'TICKER', 'CUSIP',
    
    # VIX
    'VIX',

    # ACCOUNTING VARIABLES
    # Net Income
    'NETINCOMEQ', 'NETINCOMETTM',
    'NETINCOMEGROWTHQ', 'NETINCOMEGROWTHTTM',

    # Operating Income (~EBIT)
    'OPERATINGINCOMEQ', 'OPERATINGINCOMETTM',
    'OPERATINGINCOMEGROWTHQ', 'OPERATINGINCOMEGROWTHTTM',

    # EBIT
    'EBITQ', 'EBITTTM',
    'EBITGROWTHQ', 'EBITGROWTHTTM',

    # Sales
    'SALESQ', 'SALESTTM',
    'SALESGROWTHQ', 'SALESGROWTHTTM',

    # Free Cash
    'FREECASHFLOWQ', 'FREECASHFLOWTTM',
    'FREECASHFLOWGROWTHQ', 'FREECASHFLOWGROWTHTTM',

    # OTHER
    'GROSSMARGINQ', 'GROSSMARGINTTM',
    'GROSSPROFASSET', 'ASSETS',

    # RATIOS
    'EBITDAMARGIN', 'CASHEV', 'PE',
    'FCFMARKETCAP',
    
    # STARMINE
    'ARM', 'ARMREVENUE', 'ARMRECS', 'ARMEARNINGS', 'ARMEXRECS',
    'EPSESTIMATEFQ', 'EPSSURPRISEFQ', 'EBITDAESTIMATEFQ', 'EBITDASURPRISEFQ',
    'REVENUEESTIMATEFQ', 'REVENUESURPRISEFQ', 'SESPLITFACTOR',
    'SIRANK', 'SIMARKETCAPRANK', 'SISECTORRANK',
    'SIUNADJRANK', 'SISHORTSQUEEZE', 'SIINSTOWNERSHIP'
  
]


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
                , PERCENT_RANK() over (
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
        elif arg[0] in ['AvgDolVol', 'MarketCap', 'SplitFactor']:
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
            from    ram.dbo.ram_etf_pricing b
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


def MIN(data_column, feature_name, length_arg, table):
    if data_column is None:
        assert "MIN requires data column"
    sqlcmd = \
        """
        select SecCode, Date_,
            (min({0}) over (
                partition by SecCode
                order by Date_
                rows between {1} preceding and current row)) as {2}
        from {3} A
        """.format(data_column, length_arg-1, feature_name, table)
    return clean_sql_cmd(sqlcmd)


def MAX(data_column, feature_name, length_arg, table):
    if data_column is None:
        assert "MAX requires data column"
    sqlcmd = \
        """
        select SecCode, Date_,
            (max({0}) over (
                partition by SecCode
                order by Date_
                rows between {1} preceding and current row)) as {2}
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


def _MASTER_ID_FIELD(feature, feature_name, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    M.{0} as {1}
        from        {2} A
        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate
        """.format(feature, feature_name, table)
    return clean_sql_cmd(sqlcmd)

def TICKER(arg0, feature_name, arg2, table):
    return _MASTER_ID_FIELD('Ticker', feature_name, table)         


def CUSIP(arg0, feature_name, arg2, table):
    return _MASTER_ID_FIELD('Cusip', feature_name, table)  


def EARNINGSFLAG(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    isnull(B.EarningsFlag, 0) as {1}
        from        {0} A
        left join   (select IdcCode, ReportDate, 1 as EarningsFlag
                     from ram.dbo.ram_equity_report_dates) B
            on      A.IdcCode = B.IdcCode
            and     A.Date_ = B.ReportDate
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def EARNINGSRETURN(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    isnull(B.EarningsReturn - B.EarningsReturnHedge, 0) as {1}
        from        {0} A
        left join   ram.dbo.ram_equity_report_dates B
            on      A.IdcCode = B.IdcCode
            and     B.ReportDate = (select max(ReportDate)
                        from ram.dbo.ram_equity_report_dates
                        where IdcCode = A.IdcCode and ReportDate < A.Date_)
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


# ~~~~~~ Accounting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _ACCOUNTING_FRAMEWORK(feature, feature_name, table):
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
        left join   ram.dbo.ram_compustat_accounting_derived B
            on      G.GVKey = B.GVKey
            and     B.ItemName = '{2}'
            and     B.AsOfDate = (select max(d.AsOfDate)
                        from ram.dbo.ram_compustat_accounting_derived d
                        where d.GVKey = G.GVKey and d.AsOfDate < A.Date_)
        """.format(table, feature_name, feature)
    return clean_sql_cmd(sqlcmd)


def SALESQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESQ', feature_name, table)


def SALESTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESTTM', feature_name, table)


def SALESGROWTHQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESGROWTHQ', feature_name, table)


def SALESGROWTHTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESGROWTHTTM', feature_name, table)


def NETINCOMEQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('NETINCOMEQ', feature_name, table)


def NETINCOMETTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('NETINCOMETTM', feature_name, table)


def NETINCOMEGROWTHQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('NETINCOMEGROWTHQ', feature_name, table)


def NETINCOMEGROWTHTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('NETINCOMEGROWTHTTM', feature_name, table)


def OPERATINGINCOMEQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('OPERATINGINCOMEQ', feature_name, table)


def OPERATINGINCOMETTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('OPERATINGINCOMETTM', feature_name, table)


def OPERATINGINCOMEGROWTHQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('OPERATINGINCOMEGROWTHQ',
                                 feature_name, table)


def OPERATINGINCOMEGROWTHTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('OPERATINGINCOMEGROWTHTTM',
                                 feature_name, table)


def EBITQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('EBITQ', feature_name, table)


def EBITTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('EBITTTM', feature_name, table)


def EBITGROWTHQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('EBITGROWTHQ', feature_name, table)


def EBITGROWTHTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('EBITGROWTHTTM', feature_name, table)


def SALESQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESQ', feature_name, table)


def SALESTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESTTM', feature_name, table)


def SALESGROWTHQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESGROWTHQ', feature_name, table)


def SALESGROWTHTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('SALESGROWTHTTM', feature_name, table)


def FREECASHFLOWQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('FREECASHFLOWQ', feature_name, table)


def FREECASHFLOWTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('FREECASHFLOWTTM', feature_name, table)


def FREECASHFLOWGROWTHQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('FREECASHFLOWGROWTHQ', feature_name, table)


def FREECASHFLOWGROWTHTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('FREECASHFLOWGROWTHTTM', feature_name, table)


def GROSSMARGINQ(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('X_GROSSMARGINQ', feature_name, table)


def GROSSMARGINTTM(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('X_GROSSMARGINTTM', feature_name, table)


def GROSSPROFASSET(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('X_GROSSPROFASSET', feature_name, table)


def ASSETS(arg0, feature_name, arg2, table):
    return _ACCOUNTING_FRAMEWORK('ASSETS', feature_name, table)


# ~~~~~~  Accounting Ratios ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _ACCOUNTING_RATIO(feature_name, numerator, denominator, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B1.Value_ / nullif(B2.Value_, 0) as {1}
        from        {0} A

        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting_derived B1
            on      G.GVKey = B1.GVKey
            and     B1.ItemName = '{2}'
            and     B1.AsOfDate = (select max(d.AsOfDate)
                        from ram.dbo.ram_compustat_accounting_derived d
                        where d.GVKey = G.GVKey and d.AsOfDate < A.Date_)

        left join   ram.dbo.ram_compustat_accounting_derived B2
            on      G.GVKey = B2.GVKey
            and     B2.ItemName = '{3}'
            and     B2.AsOfDate = (select max(d.AsOfDate)
                        from ram.dbo.ram_compustat_accounting_derived d
                        where d.GVKey = G.GVKey and d.AsOfDate < A.Date_)
        """.format(table, feature_name, numerator, denominator)
    return clean_sql_cmd(sqlcmd)


def EBITDAMARGIN(arg0, feature_name, arg2, table):
    return _ACCOUNTING_RATIO(feature_name, 'OPERATINGINCOMETTM',
                             'SALESTTM', table)


# ~~~~~~ Custom Accounting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def CASHEV(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B2.Value_ / nullif(A.MarketCap + B1.Value_ - B2.Value_, 0) as {1}
        from        {0} A

        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting_derived B1
            on      G.GVKey = B1.GVKey
            and     B1.ItemName = 'SHORTLONGDEBT'
            and     B1.AsOfDate = (select max(d.AsOfDate)
                        from ram.dbo.ram_compustat_accounting_derived d
                        where d.GVKey = G.GVKey and d.AsOfDate < A.Date_)

        left join   ram.dbo.ram_compustat_accounting_derived B2
            on      G.GVKey = B2.GVKey
            and     B2.ItemName = 'X_CASHANDSECURITIES'
            and     B2.AsOfDate = (select max(d.AsOfDate)
                        from ram.dbo.ram_compustat_accounting_derived d
                        where d.GVKey = G.GVKey and d.AsOfDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


def FCFMARKETCAP(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B1.Value_ / nullif(A.MarketCap, 0) as {1}
        from        {0} A

        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting_derived B1
            on      G.GVKey = B1.GVKey
            and     B1.ItemName = 'FREECASHFLOWTTM'
            and     B1.AsOfDate = (select max(d.AsOfDate)
                        from ram.dbo.ram_compustat_accounting_derived d
                        where d.GVKey = G.GVKey and d.AsOfDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


# ~~~~~~ Pricing and Accounting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def PE(arg0, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    A.MarketCap / nullif(B.Value_, 0) as {1}
        from        {0} A

        join        ram.dbo.ram_master_ids M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate

        left join   ram.dbo.ram_idccode_to_gvkey_map G
            on      M.IdcCode = G.IdcCode
            and     A.Date_ between G.StartDate and G.EndDate

        left join   ram.dbo.ram_compustat_accounting_derived B
            on      G.GVKey = B.GVKey
            and     B.ItemName = 'NETINCOMETTM'
            and     B.AsOfDate = (select max(ReportDate) d
                        from ram.dbo.ram_compustat_accounting d
                        where d.GVKey = G.GVKey and d.ReportDate < A.Date_)
        """.format(table, feature_name)
    return clean_sql_cmd(sqlcmd)


# ~~~~~~ SPY ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def MKT(data_column, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.{0} as {1}
        from        {2} A
        left join   (select  b.Date_, b.{0}
                     from ram.dbo.ram_etf_pricing b
                     where b.SecCode = 61494) B
            on       A.Date_ = B.Date_
        """.format(data_column, feature_name, table)
    return clean_sql_cmd(sqlcmd)


# ~~~~~~ VIX ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def VIX(data_column, feature_name, arg2, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.Close_ as {0}
        from        {1} A
        left join   (select  b.Date_, b.Close_
                     from ram.dbo.ram_index_pricing b
                     where b.IdcCode = 101506) B
            on       A.Date_ = B.Date_
        """.format(feature_name, table)
    return clean_sql_cmd(sqlcmd)


# ~~~~~ STARMINE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def _STARMINE_ARM(feature, feature_name, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.{2} as {1}
        from        {0} A
        join        ram.dbo.ram_starmine_map M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate
        left join   ram.dbo.ram_starmine_arm B
            on      M.SecId = B.SecId
            and     B.AsOfDate = A.Date_
        """.format(table, feature_name, feature)
    return clean_sql_cmd(sqlcmd)

  
def _STARMINE_SMART_ESTIMATE(feature, feature_name, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.{2} as {1}
        from        {0} A
        join        ram.dbo.ram_starmine_map M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate
        left join   ram.dbo.ram_starmine_smart_estimate B
            on      M.SecId = B.SecId
            and     B.AsOfDate = A.Date_
        """.format(table, feature_name, feature)
    return clean_sql_cmd(sqlcmd)


def _STARMINE_SI(feature, feature_name, table):
    sqlcmd = \
        """
        select      A.SecCode,
                    A.Date_,
                    B.{2} as {1}
        from        {0} A
        join        ram.dbo.ram_starmine_map M
            on      A.SecCode = M.SecCode
            and     A.Date_ between M.StartDate and M.EndDate
        left join   ram.dbo.ram_starmine_short_interest B
            on      M.SecId = B.SecId
            and     B.AsOfDate = A.Date_
        """.format(table, feature_name, feature)
    return clean_sql_cmd(sqlcmd)

 
def ARM(arg0, feature_name, arg2, table):
    return _STARMINE_ARM('ARMScore', feature_name, table)

  
def ARMREVENUE(arg0, feature_name, arg2, table):
    return _STARMINE_ARM('ARMRevComp', feature_name, table)

  
def ARMRECS(arg0, feature_name, arg2, table):
    return _STARMINE_ARM('ARMRecsComp', feature_name, table)

  
def ARMEARNINGS(arg0, feature_name, arg2, table):
    return _STARMINE_ARM('ARMPrefErnComp', feature_name, table)

  
def ARMEXRECS(arg0, feature_name, arg2, table):
    return _STARMINE_ARM('ARMScoreExRecs', feature_name, table)


def SESPLITFACTOR(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SplitFactor', feature_name, table)

  
def EPSESTIMATEFQ(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SE_EPS_FQ{}'.format(arg2), feature_name,
                                    table)

def EPSSURPRISEFQ(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SE_EPS_Surprise_FQ{}'.format(arg2),
                                    feature_name, table)

def EBITDAESTIMATEFQ(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SE_EBITDA_FQ{}'.format(arg2),
                                    feature_name, table)

def EBITDASURPRISEFQ(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SE_EBITDA_Surprise_FQ{}'.format(arg2),
                                    feature_name, table)

def REVENUEESTIMATEFQ(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SE_REV_FQ{}'.format(arg2),
                                    feature_name, table)

def REVENUESURPRISEFQ(arg0, feature_name, arg2, table):
    return _STARMINE_SMART_ESTIMATE('SE_REV_Surprise_FQ{}'.format(arg2),
                                    feature_name, table)

def SIRANK(arg0, feature_name, arg2, table):
    return _STARMINE_SI('SI_Rank', feature_name, table)

  
def SIMARKETCAPRANK(arg0, feature_name, arg2, table):
    return _STARMINE_SI('SI_MarketCapRank', feature_name, table)

  
def SISECTORRANK(arg0, feature_name, arg2, table):
    return _STARMINE_SI('SI_SectorRank', feature_name, table)

  
def SIUNADJRANK(arg0, feature_name, arg2, table):
    return _STARMINE_SI('SI_UnAdjRank', feature_name, table)

  
def SISHORTSQUEEZE(arg0, feature_name, arg2, table):
    return _STARMINE_SI('SI_ShortSqueeze', feature_name, table)

  
def SIINSTOWNERSHIP(arg0, feature_name, arg2, table):
    return _STARMINE_SI('SI_InstOwnership', feature_name, table)


# ~~~~~~ Utility ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
