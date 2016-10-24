import datetime as dt
import dateutil.parser


def check_input_date(date):
    """
    Utility function to convert inputted dates from user to datetime.date
    """
    if isinstance(date, dt.datetime):
        return date.date()
    if isinstance(date, str):
        return dateutil.parser.parse(date).date()
    return date
