"""
utilities for processing dates. 

Created on Jul 12, 2013
@author: zul110
"""


from datetime import datetime
from collections import namedtuple
from datetime import date
import re


DATETIME_REGEX = re.compile('^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(T|\s+)(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).*?$')
VERSIONING_TIMESTAMP = "%Y-%m-%d-%H-%M"


def overlap_for_two_date_ranges(range1Start, range1End,
                                range2Start, range2End):
    Range = namedtuple('Range', ['start', 'end'])
    r1 = Range(start=range1Start, end=range1End)
    r2 = Range(start=range2Start, end=range2End)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    overlap = (earliest_end - latest_start).days + 1
    if overlap < 0:
        overlap = 0
    return overlap


def overlap_ratio_for_two_date_ranges(range1Start, range1End, range2Start, range2End):
    Range = namedtuple('Range', ['start', 'end'])
    r1 = Range(start=range1Start, end=range1End)
    r2 = Range(start=range2Start, end=range2End)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    overlap = (earliest_end - latest_start).days + 1
    if overlap < 0:
        overlap = 0
    earliest_start = min(r1.start, r2.start)
    latest_end = max(r1.end, r2.end)
    whole_range = (latest_end - earliest_start).days + 1
    if whole_range == 0:
        return 0
    else:
        return (overlap * 1.0) / (whole_range * 1.0)


def str_to_date(dateStr, tformat):
    """
    example: strToDate('24052010', "%d%m%Y")
    """
    return datetime.strptime(dateStr, tformat).date()


def date_to_str(dtime, tformat):
    """
    example:
    >> a = datetime.datetime.strptime('2012-03-04', "%Y-%m-%d")
    dateToStr(a, "%Y-%m-%d")
    """
    return dtime.strftime(tformat)


def tuple_to_date(year, month, day):
    return date(year, month, day)


def tuple_to_date_time(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second)


def get_versioning_timestamp():
    vtimestamp = datetime.now().strftime(VERSIONING_TIMESTAMP)
    return vtimestamp


def get_latest_versioning_timestamp(vtimestamps):
    """
    find the latest timestamp
    """
    l = []
    for vt in vtimestamps:
        l.append(datetime.strptime(vt, VERSIONING_TIMESTAMP))
    return max(l).strftime("%Y-%m-%d-%H-%M")


def test():
    range2Start = str_to_date('24-05-2010', "%d-%m-%Y")
    range2End = str_to_date('26-06-2010', "%d-%m-%Y")
    range1Start = str_to_date('24-07-2010', "%d-%m-%Y")
    range1End = str_to_date('24-08-2010', "%d-%m-%Y")
    print overlap_for_two_date_ranges(range1Start, range1End, range2Start, range2End)

if __name__ == "__main__":
    l = ['2013-12-27-21-29', '2013-12-27-20-30', '2013-12-24-20-30']
    print get_latest_versioning_timestamp(l)
    test()