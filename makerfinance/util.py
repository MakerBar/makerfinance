from datetime import datetime
import dateutil.parser
from decimal import Decimal, InvalidOperation
from exceptions import TypeError, ValueError
import uuid

__author__ = 'andriod'

def adjust_total(total, subtotal, adjustment):
    """
    Adjust the total on a partially reimbursed receipt by subtracting non-reimbursed expenses and proportionately
    adjusting tax and shipping
    """
    return total * (float(subtotal - adjustment) / subtotal)


def encode(to_encode, epsilon=False):
    if hasattr(to_encode, "isoformat"):
        if isinstance(to_encode, datetime):
            # if this is really a date
            if to_encode.hour == 0 and to_encode.minute == 0 and to_encode.second == 0:
                to_encode = to_encode.date()
            elif not epsilon:
                return to_encode.replace(microsecond=0).isoformat()
            else:
                return to_encode.isoformat()
        return to_encode.isoformat()
    elif isinstance(to_encode, bool):
        return unicode(to_encode)
    try:
        return "%.2f" % to_encode
    except TypeError:
        return unicode(to_encode)


def decode(string):
    if string == "False":
        return False
    if not len(string):
        return string
    try:
        return Decimal(string)
    except InvalidOperation:
        try:
            return dateutil.parser.parse(string)
        except ValueError:
            return string


def mk_id():
    return uuid.uuid4().hex