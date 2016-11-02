
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
    if feature[:4] == 'PRMA':
        return _prma(feature)
    elif feature[:4] == 'BOLL':
        return _bollinger(feature)
    elif feature[:3] == 'ADJ':
        return _adjust_price(feature)
    elif feature[:6] == 'LAGADJ':
        return _lag_price(feature)
    elif feature[:3] == 'VOL':
        return _vol(feature)
    elif feature in MASTER_COLS:
        return feature, feature
    else:
        pass


###############################################################################
# Technical variables - MUST RETURN TWO STRINGS FOR TWO SEQUENTIAL QUERIES

def _prma(feature):
    assert feature[:4] == 'PRMA'
    days = int(feature[4:])
    prma_string = \
        """
        {0}/{1} as {2}
        """.format(_adj('Close_'), _rolling_avg(days), feature)
    return prma_string, feature


def _bollinger(feature):
    """
    Re-write this to be clearer.
    """
    assert feature[:4] == 'BOLL'
    days = int(feature[4:])
    # Get the high and low side of the for the bollinger band
    lowstr = "({0} - 2 * {1})".format(_rolling_avg(days), _rolling_std(days))
    highstr = "({0} + 2 * {1})".format(_rolling_avg(days), _rolling_std(days))
    bollinger_string = \
        """
        ({0} - {1}) / nullif({2} - {1}, 0) as {3}
        """.format(_adj('Close_'), lowstr, highstr, feature)
    return bollinger_string, feature


def _vol(feature):
    assert feature[:3] == 'VOL'
    days = int(feature[3:])
    # PART 1 - For CTE
    vol_string = \
        """
        {1} as CloseVOL{0},
        {2} as LagCloseVOL{0}
        """.format(days, _adj('Close_'), _lag(_adj('Close_'), 1))
    # PART 2 - For query
    vol_string2 = \
        """
        stdev(CloseVOL{0} / LagCloseVOL{0}) over (
            partition by IdcCode
            order by Date_
            rows between {1} preceding and current row) as {2}
        """.format(days, days - 1, feature)
    return vol_string, vol_string2


def _adjust_price(feature):
    assert feature[:3] == 'ADJ'
    return "{0} as {1} ".format(_adj(feature[3:]), feature), feature


def _lag_adj_price(feature):
    assert feature[:6] == 'LAGADJ'
    return "{0} as {1} ".format(_lag(_adj(feature[6:]), 1), feature), feature


###############################################################################
# HELPERS

def _adj(feature):
    assert feature in ['Open_', 'High', 'Low', 'Close_', 'Vwap']
    return "({0} * DividendFactor * SplitFactor)".format(feature)


def _rolling_avg(days):
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
        """.format(_adj('Close_'), offset)
    return avg_string


def _rolling_std(days):
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
        """.format(_adj('Close_'), offset)
    return std_string


def _lag(col, offset):
    lag_string = \
        """
        (Lag({0}, {1}) over (
            partition by IdcCode
            order by Date_))
        """.format(col, offset)
    return lag_string
