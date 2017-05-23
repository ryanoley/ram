import numpy as np
import datetime as dt
import dateutil.parser


def check_input_date(date):
    """
    Utility function to convert inputted dates from user to
    datetime.datetime with 0s for hours and minutes
    """
    if isinstance(date, dt.datetime):
        return dt.datetime(date.year, date.month, date.day)
    if isinstance(date, dt.date):
        return dt.datetime(date.year, date.month, date.day)
    if isinstance(date, str):
        date = dateutil.parser.parse(date).date()
        return dt.datetime(date.year, date.month, date.day)
    return date


def convert_date_array(dates):
    # Convert to numpy array
    dates = np.array(dates)
    # Check if already converted series
    if isinstance(dates[0], dt.datetime):
        return dates
    if isinstance(dates[0], dt.date):
        return np.array([dt.datetime(t.year, t.month, t.day) for t in dates])

    # If string, get form of punctuation
    elif isinstance(dates[0], str):
        punc = ''.join(p for p in dates[0] if not p.isalnum())
    else:
        raise TypeError('Input must be array of strings or datetime.date')
    # Convert strings
    try:
        if len(punc) > 0:
            punc = punc[0]
            # Standard MarketQA input as 'mm/dd/YYYY'
            if punc[0] == '/':
                # capture specific case where one has mm/dd/YY
                if len(dates[0].split('/')[2]) == 2:
                    raise
                else:
                    str_date = lambda d: '{2:04d}-{0:02d}-{1:02d}'.format(
                        *map(int, d.split(punc)))
                dates = map(str_date, dates)
                return convert_date_array(
                    np.array(dates, dtype=np.datetime64).astype(dt.datetime))
            elif punc[0] == '-':
                str_date = lambda d: '{0:04d}-{1:02d}-{2:02d}'.format(
                    *map(int, d.split(punc)))
                dates = map(str_date, dates)
                return convert_date_array(
                    np.array(dates, dtype=np.datetime64).astype(dt.date))
            else:
                raise
        elif len(dates[0]) == 8:
            return np.array([dt.datetime(int(t[:4]), int(t[4:6]),
                                         int(t[6:])) for t in dates])
    except:
        def get_date(d):
            try:
                return dparser.parse(d, fuzzy=True,
                                     default=dt.datetime(1900, 1, 1))
            except:
                return np.nan
        out_dates = np.array(map(get_date, dates))
        # Replace null values
        ind = out_dates == dt.datetime(1900, 1, 1)
        out_dates[ind] = np.nan

    return out_dates
