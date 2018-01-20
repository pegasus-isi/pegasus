import decimal
import time

from Pegasus.service import app


@app.template_filter('lstrip')
def lstrip(value, chars=' '):
    value.lstrip(chars)


@app.template_filter('rstrip')
def rstrip(value, chars=' '):
    value.rstrip(chars)


@app.template_filter('strip')
def strip(value, chars=' '):
    value.strip(chars)


@app.template_filter('dec_to_float')
def dec_to_float(dec):
    '''
    Decimal to Float
    '''
    if dec:
        return float(dec)
    else:
        return None


@app.template_filter('time_to_date_str')
def time_to_date_str(ts):
    '''
    Change an integer duration to be represented as a data string
    '''
    return time.strftime('%Y-%m-%d Hour %H', time.localtime(ts))


@app.template_filter('to_lower_case')
def to_lower_case(str):
    '''
    Convert string to lower case
    '''
    return str.lower()


@app.template_filter('to_upper_case')
def to_upper_case(str):
    '''
    Convert string to upper case
    '''
    return str.upper()


@app.template_filter('capitalize')
def capitalize(str):
    '''
    Capitalizes first character of the String
    '''
    return str.capitalize()


@app.template_filter('time_to_str')
def time_to_str(time):
    '''
    Change an integer duration to be represented as d days h hours m mins s secs
    but only use the two major units (ie, drop seconds if hours and minutes are
    populated)
    '''
    DAY = 86400
    HOUR = 3600
    MIN = 60

    max_units = 2
    num_units = 0

    if time is None or not (
        isinstance(time, decimal.Decimal) or isinstance(time, float)
    ):
        return time

    str_time = ''
    time = int(time)

    if time >= DAY:
        temp_time = time / DAY

        if temp_time > 1:
            str_time += str(temp_time) + ' days '
        else:
            str_time += str(time / DAY) + ' day '

        time = time % DAY
        num_units += 1

    if time >= HOUR:

        temp_time = time / HOUR

        if temp_time > 1:
            str_time += str(temp_time) + ' hours '
        else:
            str_time += str(time / HOUR) + ' hour '

        time = time % HOUR
        num_units += 1

    if time >= MIN and num_units < max_units:

        temp_time = time / MIN

        if temp_time > 1:
            str_time += str(temp_time) + ' mins '
        else:
            str_time += str(time / MIN) + ' min '

        time = time % MIN
        num_units += 1

    if time > 0 and num_units < max_units:

        if time > 1:
            str_time += str(time) + ' secs '
        else:
            str_time += str(time) + ' sec '

    # 0 second duration - is this the way we want to display that?
    if str_time == '':
        str_time = '0 secs'

    return str_time.strip()
