"""
This file implements several utility functions pegasus-statistics and pegasus-plots.
"""

##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

import math
from datetime import datetime


def format_seconds(duration, max_comp=2):
    """
    Utility for converting time to a readable format
    @param duration :  time in seconds and miliseconds
    @param max_comp :  number of components of the returned time
    @return time in n component format
    """
    comp = 0
    if duration is None:
        return "-"
    math.modf(duration)[0]
    sec = int(duration)
    formatted_duration = ""
    years = sec // 31536000
    sec -= 31536000 * years
    days = sec // 86400
    sec -= 86400 * days
    hrs = sec // 3600
    sec -= 3600 * hrs
    mins = sec // 60
    sec -= 60 * mins

    # years
    if comp < max_comp and (years >= 1 or comp > 0):
        comp += 1
        if years == 1:
            formatted_duration += str(years) + " year, "
        else:
            formatted_duration += str(years) + " years, "

    # days
    if comp < max_comp and (days >= 1 or comp > 0):
        comp += 1
        if days == 1:
            formatted_duration += str(days) + " day, "
        else:
            formatted_duration += str(days) + " days, "

    # hours
    if comp < max_comp and (hrs >= 1 or comp > 0):
        comp += 1
        if hrs == 1:
            formatted_duration += str(hrs) + " hr, "
        else:
            formatted_duration += str(hrs) + " hrs, "

    # mins
    if comp < max_comp and (mins >= 1 or comp > 0):
        comp += 1
        if mins == 1:
            formatted_duration += str(mins) + " min, "
        else:
            formatted_duration += str(mins) + " mins, "

    # seconds
    if comp < max_comp and (sec >= 0 or comp > 0):
        comp += 1
        if formatted_duration == "":
            # we only have a value < 1 minute
            # takes care of rounding down to 0
            sec = round(duration, 2)

        if sec == 1:
            formatted_duration += str(sec) + " sec, "
        else:
            formatted_duration += str(sec) + " secs, "

    if formatted_duration[-2:] == ", ":
        formatted_duration = formatted_duration[:-2]

    return formatted_duration


def get_workflow_wall_time(workflow_states_list):
    """
    Utility method for returning the workflow wall time given all the workflow states
    @worklow_states_list list of all workflow states.
    """
    workflow_wall_time = None
    workflow_start_event_count = 0
    workflow_end_event_count = 0
    workflow_start_cum = 0
    workflow_end_cum = 0
    for workflow_state in workflow_states_list:
        if workflow_state.state == "WORKFLOW_STARTED":
            workflow_start_event_count += 1
            workflow_start_cum += workflow_state.timestamp
        else:
            workflow_end_event_count += 1
            workflow_end_cum += workflow_state.timestamp
    if workflow_start_event_count > 0 and workflow_end_event_count > 0:
        if workflow_start_event_count == workflow_end_event_count:
            workflow_wall_time = workflow_end_cum - workflow_start_cum
    return workflow_wall_time


def get_date_multiplier(date_filter):
    """
    Utility for returning the multiplier for a given date filter
    @param date filter :  the given date filter
    @return multiplier for a given filter
    """
    vals = {"day": 86400, "hour": 3600}
    return vals[date_filter]


def get_date_format(date_filter):
    """
    Utility for returning the date format for a given date filter
    @param date filter :  the given date filter
    @return the date format for a given filter
    """
    vals = {"day": "%Y-%m-%d", "hour": "%Y-%m-%d : %H"}
    return vals[date_filter]


def get_date_print_format(date_filter):
    """
    Utility for returning the date format for a given date filter in human readable format
    @param date filter :  the given date filter
    @return the date format for a given filter
    """
    vals = {"day": "[YYYY-MM-DD]", "hour": "[YYYY-MM-DD : HH]"}
    return vals[date_filter]


def convert_datetime_to_printable_format(timestamp, date_time_filter="hour"):
    """
    Utility for returning the date format  in human readable format
    @param timestamp :  the unix timestamp
    @param date filter :  the given date filter
    @return the date format in human readable format
    """

    local_date_time = convert_utc_to_local_datetime(timestamp)
    date_formatted = local_date_time.strftime(get_date_format(date_time_filter))
    return date_formatted


def convert_utc_to_local_datetime(utc_timestamp):
    """
    Utility for converting the timestamp  to local time
    @param timestamp :  the unix timestamp
    @return the date format in human readable format
    """
    local_datetime = datetime.fromtimestamp(utc_timestamp)
    return local_datetime


def convert_stats_to_base_time(stats_by_time, date_time_filter="hour", isHost=False):
    """
    Converts the time grouped by hour into local time.Converts the time grouped by hours into day based on the date_time_filter
    @param stats_by_time : ime grouped by hou
    @param date filter :  the given date filter
    @param isHost : true if it is grouped by host
    @return the stats list after doing conversion
    """
    formatted_stats_by_hour_list = []
    for stats in stats_by_time:
        timestamp = stats.date_format * get_date_multiplier("hour")
        formatted_stats = {}
        formatted_stats["timestamp"] = timestamp
        formatted_stats["count"] = stats.count
        formatted_stats["runtime"] = stats.total_runtime
        if isHost:
            formatted_stats["host"] = stats.host_name
        formatted_stats_by_hour_list.append(formatted_stats)
        formatted_stats = None

    if date_time_filter == "hour":
        for formatted_stats in formatted_stats_by_hour_list:
            formatted_stats["date_format"] = convert_datetime_to_printable_format(
                formatted_stats["timestamp"], date_time_filter
            )
        return formatted_stats_by_hour_list
    else:
        day_to_hour_mapping = {}
        formatted_stats_by_day_list = []
        for formatted_stats_by_hour in formatted_stats_by_hour_list:
            formatted_stats_by_day = None
            corresponding_day = convert_datetime_to_printable_format(
                formatted_stats_by_hour["timestamp"], date_time_filter
            )
            id = ""
            if isHost:
                id += formatted_stats_by_hour["host"] + ":"
            id += corresponding_day
            if id in day_to_hour_mapping:
                formatted_stats_by_day = day_to_hour_mapping[id]
                formatted_stats_by_day["count"] += formatted_stats_by_hour["count"]
                formatted_stats_by_day["runtime"] += formatted_stats_by_hour["runtime"]
            else:
                formatted_stats_by_day = formatted_stats_by_hour
                formatted_stats_by_day["date_format"] = corresponding_day
                day_to_hour_mapping[id] = formatted_stats_by_day
                formatted_stats_by_day_list.append(formatted_stats_by_day)
        return formatted_stats_by_day_list


def round_decimal_to_str(value, to=3):
    """
    Utility method for rounding the decimal value to string to given digits
    @param value :  value to round
    @param to    :  how many decimal points to round to
    """
    rounded_value = "-"
    if value is None:
        return rounded_value
    rounded_value = str(round(float(value), to))
    return rounded_value
