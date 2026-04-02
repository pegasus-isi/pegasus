import decimal
import time


def lstrip(value, chars=" "):
    value.lstrip(chars)


def rstrip(value, chars=" "):
    value.rstrip(chars)


def strip(value, chars=" "):
    value.strip(chars)


def dec_to_float(dec):
    """
    Decimal to Float
    """
    if dec:
        return float(dec)
    else:
        return None


def time_to_date_str(ts, fmt="%Y-%m-%d Hour %H"):
    """
    Change an integer duration to be represented as a date string
    """
    return time.strftime(fmt, time.localtime(ts))


def to_lower_case(str):
    """
    Convert string to lower case
    """
    return str.lower()


def to_upper_case(str):
    """
    Convert string to upper case
    """
    return str.upper()


def capitalize(str):
    """
    Capitalizes first character of the String
    """
    return str.capitalize()


def time_to_str(time):
    """
    Change an integer duration to be represented as d days h hours m mins s secs
    but only use the two major units (ie, drop seconds if hours and minutes are
    populated)
    """
    DAY = 86400
    HOUR = 3600
    MIN = 60

    max_units = 2
    num_units = 0

    if time is None or not (
        isinstance(time, decimal.Decimal) or isinstance(time, float)
    ):
        return time

    str_time = ""
    time = int(time)

    if time >= DAY:
        temp_time = time // DAY

        if temp_time > 1:
            str_time += str(temp_time) + " days "
        else:
            str_time += str(temp_time) + " day "

        time = time % DAY
        num_units += 1

    if time >= HOUR:

        temp_time = time // HOUR

        if temp_time > 1:
            str_time += str(temp_time) + " hours "
        else:
            str_time += str(temp_time) + " hour "

        time = time % HOUR
        num_units += 1

    if time >= MIN and num_units < max_units:

        temp_time = time // MIN

        if temp_time > 1:
            str_time += str(temp_time) + " mins "
        else:
            str_time += str(temp_time) + " min "

        time = time % MIN
        num_units += 1

    if time > 0 and num_units < max_units:

        if time > 1:
            str_time += str(time) + " secs "
        else:
            str_time += str(time) + " sec "

    # 0 second duration - is this the way we want to display that?
    if str_time == "":
        str_time = "0 secs"

    return str_time.strip()


def register_jinja2_filters(app):
    app.add_template_filter(lstrip, "lstrip")
    app.add_template_filter(rstrip, "rstrip")
    app.add_template_filter(strip, "strip")
    app.add_template_filter(dec_to_float, "dec_to_float")
    app.add_template_filter(time_to_date_str, "time_to_date_str")
    app.add_template_filter(to_lower_case, "to_lower_case")
    app.add_template_filter(to_upper_case, "to_upper_case")
    app.add_template_filter(capitalize, "capitalize")
    app.add_template_filter(time_to_str, "time_to_str")
