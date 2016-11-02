
# Allowable columns
MASTER_COLS = ['Open_', 'High', 'Low', 'Close_',
               'Vwap', 'Volume', 'AvgDolVol', 'MarketCap',
               'CashDividend', 'SplitFactor']


def sqlcmd_from_feature_list(features):
    """
    Function facing the data handler. Returns the features that are
    able to be queried, and the sql command to get those columns.
    """
    if not isinstance(features, list):
        features = [features]

    cte_out = []
    body_out = []
    feature_out = []
    for f in features:
        f2 = _get_feature_string(f)
        if f2:
            feature_out.append(f)
            cte_out.append(f2[0])
            body_out.append(f2[1])
    return feature_out, ', '.join(cte_out), ', '.join(body_out)


def _get_feature_string(feature):
    # Split features
    sp = feature.split('_')
    if sp[0] == 'PRMA':
        return _prma(feature)
    elif sp[0] == 'MA':
        return _ma(feature)
    elif sp[0] == 'LAGMA':
        return _lag_ma(feature)
    elif sp[0] == 'BOLL':
        return _bollinger(feature)
    elif sp[0] == 'ADJ':
        return _adjust_price(feature)
    elif sp[0] == 'LAGADJ':
        return _lag_adj_price(feature)
    elif sp[0] == 'VOL':
        return _vol(feature)
    elif sp[0] == 'LAGVOL':
        return _vol(feature, True)
    elif sp[0] == 'P':
        return _actual_price(feature)
    else:
        pass


###############################################################################
# Technical variables - MUST RETURN TWO STRINGS FOR TWO SEQUENTIAL QUERIES

def _prma(feature):
    sp = feature.split('_')
    days = int(sp[1])
    col = sp[2]
    if col in ['Open', 'Close']:
        col = col + '_'
    prma_string = \
        """
        {0}/{1} as {2}
        """.format(_adj(col), _rolling_avg(col, int(days)), feature)
    return prma_string, feature


def _ma(feature):
    sp = feature.split('_')
    days = int(sp[1])
    col = sp[2]
    if col in ['Open', 'Close']:
        col = col + '_'
    ma_string = \
        """
        {0} as {1}
        """.format(_rolling_avg(col, int(days)), feature)
    return ma_string, feature


def _lag_ma(feature):
    sp = feature.split('_')
    days = int(sp[1])
    col = sp[2]
    if col in ['Open', 'Close']:
        col = col + '_'
    # Part I: Moving Average
    ma_string = \
        """
        {0} as {1}
        """.format(_rolling_avg(col, int(days)), feature+'temp')
    # Part II: Lag MA
    lag_string = \
        """
        {0} as {1}
        """.format(_lag(feature+'temp', 1), feature)

    return ma_string, lag_string


def _bollinger(feature):
    """
    Re-write this to be clearer.
    """
    sp = feature.split('_')
    days = int(sp[1])
    col = sp[2]
    # Adjustment for Close/Open columns
    if col in ['Open', 'Close']:
        col = col + '_'
    # Get the high and low side of the for the bollinger band
    lowstr = "({0} - 2 * {1})".format(
        _rolling_avg(col, days), _rolling_std(col, days))
    highstr = "({0} + 2 * {1})".format(
        _rolling_avg(col, days), _rolling_std(col, days))
    bollinger_string = \
        """
        ({0} - {1}) / nullif({2} - {1}, 0) as {3}
        """.format(_adj(col), lowstr, highstr, feature)
    return bollinger_string, feature


def _vol(feature, lag=False):
    sp = feature.split('_')
    days = int(sp[1])
    col = sp[2]
    # Adjustment for Close/Open columns
    if col in ['Open', 'Close']:
        col = col + '_'
    # PART 1 - For CTE
    # PART 2 - For query
    if lag:
        vol_string = \
            """
            {1} as {3}LAGVOL{0},
            {2} as Lag{3}LAGVOL{0}
            """.format(days, _adj(col), _lag(_adj(col), 1), col)
        vol_string2 = \
            """
            stdev({3}LAGVOL{0} / Lag{3}LAGVOL{0}) over (
                partition by IdcCode
                order by Date_
                rows between {1} preceding and 1 preceding) as {2}
            """.format(days, days, feature, col)
    else:
        vol_string = \
            """
            {1} as {3}VOL{0},
            {2} as Lag{3}VOL{0}
            """.format(days, _adj(col), _lag(_adj(col), 1), col)
        vol_string2 = \
            """
            stdev({3}VOL{0} / Lag{3}VOL{0}) over (
                partition by IdcCode
                order by Date_
                rows between {1} preceding and current row) as {2}
            """.format(days, days - 1, feature, col)
    return vol_string, vol_string2


def _adjust_price(feature):
    sp = feature.split('_')
    col = sp[1]
    # Adjustment for Close/Open columns
    if col in ['Open', 'Close']:
        col = col + '_'
    return "{0} as {1} ".format(_adj(col), feature), feature


def _lag_adj_price(feature):
    sp = feature.split('_')
    days = int(sp[1])
    col = sp[2]
    # Adjustment for Close/Open columns
    if col in ['Open', 'Close']:
        col = col + '_'
    return "{0} as {1} ".format(_lag(_adj(col), days), feature), feature


def _actual_price(feature):
    sp = feature.split('_')
    col = sp[1]
    # Adjustment for Close/Open columns
    if col in ['Open', 'Close']:
        col = col + '_'
    return "{0} as {1} ".format(col, feature), feature


###############################################################################
# HELPERS

def _adj(feature):
    assert feature in ['Open_', 'High', 'Low', 'Close_', 'Vwap']
    return "({0} * DividendFactor * SplitFactor)".format(feature)


def _rolling_avg(col, days):
    """
    Will always adjust prices because of the time component
    """
    offset = days - 1
    avg_string = \
        """
        (avg({0}) over
            (partition by IdcCode
             order by Date_
             rows between {1} preceding and current row))
        """.format(_adj(col), offset)
    return avg_string


def _rolling_std(col, days):
    """
    Will always adjust prices because of the time component
    """
    offset = days - 1
    std_string = \
        """
        (stdev({0}) over (
            partition by IdcCode
            order by Date_
            rows between {1} preceding and current row))
        """.format(_adj(col), offset)
    return std_string


def _lag(col, offset):
    lag_string = \
        """
        (Lag({0}, {1}) over (
            partition by IdcCode
            order by Date_))
        """.format(col, offset)
    return lag_string
