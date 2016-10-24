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
