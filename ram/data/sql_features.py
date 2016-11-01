
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

    out = []
    fout = []
    for f in features:
        f2 = _get_feature_string(f)
        if f2:
            out.append(f2)
            fout.append(f)
    return fout, ', '.join(out)


def _get_feature_string(feature):
    if feature[:4] == 'PRMA':
        return _prma(feature)
    elif feature[:4] == 'BOLL':
        return _bollinger(feature)
    elif feature[:3] == 'ADJ':
        return _adj(feature)
    elif feature in MASTER_COLS:
        return feature
    else:
        pass


###############################################################################
# Technical variables

def _prma(feature):
    assert feature[:4] == 'PRMA'
    days = int(feature[4:])
    prma_string = \
        "(Close_ * DividendFactor * SplitFactor) /" + \
        _rolling_avg(days) + "as {0}".format(feature)
    return prma_string


def _bollinger(feature):
    assert feature[:4] == 'BOLL'
    days = int(feature[4:])
    # Get the high and low side of the for the bollinger band
    lowstr = "(" + _rolling_avg(days) + '- 2 *' + _rolling_std(days) + ")"
    highstr = "(" + _rolling_avg(days) + '+ 2 *' + _rolling_std(days) + ")"
    bollinger_string = \
        "( (Close_ * DividendFactor * SplitFactor) - " + lowstr + ") / " + \
        "nullif((" + highstr + " - " + lowstr + "), 0) as {0}".format(feature)
    return bollinger_string


def _adj(feature):
    assert feature[:3] == 'ADJ'
    col = feature[3:]
    assert col in MASTER_COLS
    return " ({0} * DividendFactor * SplitFactor) as {1} ".format(col, feature)


###############################################################################
# HELPERS

def _rolling_avg(days):
    offset = days - 1
    avg_string = " (avg(Close_ * DividendFactor * SplitFactor) over " + \
        "(partition by IdcCode order by Date_ " + \
        "rows between {0} preceding and current row)) ".format(offset)
    return avg_string


def _rolling_std(days):
    offset = days - 1
    avg_string = " (stdev(Close_ * DividendFactor * SplitFactor) over " + \
        "(partition by IdcCode order by Date_ " + \
        "rows between {0} preceding and current row)) ".format(offset)
    return avg_string
