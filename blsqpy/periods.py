
import math
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parseDate
from abc import ABC, abstractmethod


class DateRange():
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def first(self):
        return self.start


class ExtractPeriod:
    def call(self, date_range):
        array = []
        current_date = self.first_date(date_range)
        while True:
            array.append(self.dhis2_format(current_date))
            current_date = self.next_date(current_date)
            if(current_date > date_range.end):
                break

        return array

    @abstractmethod
    def first_date(self, date_range):
        pass

    @abstractmethod
    def dhis2_format(self, date_range):
        pass

    @abstractmethod
    def next_date(self, date_range):
        pass


class ExtractMonthlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(months=1)

    def dhis2_format(self, date):
        return date.strftime("%Y%m")

    def first_date(self, date_range):
        return date_range.start.replace(day=1)


class ExtractQuarterlyPeriod(ExtractPeriod):
    def next_date(self, current_date):
        quarterStart = date(current_date.year,
                            (current_date.month - 1) // 3 * 3 + 1, 1)
        nextDate = quarterStart + relativedelta(months=3)
        return nextDate

    def dhis2_format(self, current_date):
        return current_date.strftime("%Y") + "Q" + str(math.ceil((current_date.month / 3.0)))

    def first_date(self, date_range):
        dt = date_range.start
        return date(dt.year, (dt.month - 1) // 3 * 3 + 1, 1)


class ExtractYearlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date.replace(year=date.year + 1, month=1, day=1)

    def dhis2_format(self, date):
        return date.strftime("%Y")

    def first_date(self, date_range):
        return date_range.start.replace(month=1, day=1)


CLASSES_MAPPING = {
    "monthly": ExtractMonthlyPeriod,
    "quarterly": ExtractQuarterlyPeriod,
    "yearly": ExtractYearlyPeriod,
}


class YearParser:
    @staticmethod
    def parse(period):
        if (len(period) != 4):
            return
        year = int(period[0:4])
        start_date = date(year=year, month=1, day=1)
        end_date = start_date.replace(month=12, day=31)

        return DateRange(start_date, end_date)


class YearQuarterParser:
    @staticmethod
    def parse(period):
        if "Q" not in period:
            return
        components = period.split("Q")
        quarter = int(components[1])
        year = int(components[0])
        month_start = (3 * (quarter - 1)) + 1
        month_end = month_start+2
        start_date = date(year=year, month=month_start, day=1)
        end_date = date(year=year, month=month_end, day=1)

        return DateRange(start_date, end_date)


class YearMonthParser:
    @staticmethod
    def parse(period):
        if len(period) != 6:
            return
        year = int(period[0:4])
        month = int(period[4:6])
        start_date = date(year=year, month=month, day=1)
        end_date = start_date + relativedelta(day=31)

        return DateRange(start_date, end_date)


PARSERS = [YearParser, YearQuarterParser,
           YearMonthParser]


class Periods:
    @staticmethod
    def split(period, frequency):
        date_range = Periods.as_date_range(period)
        mapper = CLASSES_MAPPING[frequency]
        periods = mapper().call(date_range)
        return periods

    @staticmethod
    def as_date_range(period):
        dateRange = None
        for parser in PARSERS:
            dateRange = parser.parse(period)
            if dateRange:
                break
        return dateRange
