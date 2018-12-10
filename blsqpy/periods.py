
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parseDate

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
            if(current_date > date_range.last):
                break

        return array


class ExtractMonthlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(months=1)

    def dhis2_format(self, date):
        return date.strftime("%Y%m")

    def first_date(self, date_range):
        return date_range.first.beginning_of_month


class ExtractQuarterlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date.next_quarter

    def dhis2_format(self, date):
        return date.strftime("%Y") + "Q" + (date.month / 3.0).ceil.to_s

    def first_date(self, date_range):
        return date_range.first.beginning_of_quarter


class ExtractYearlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date.next_year

    def dhis2_format(self, date):
        return date.strftime("%Y")

    def first_date(self, date_range):
        return date_range.first.beginning_of_year


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
        year = period[0:3]
        start_date = parseDate("#{year}-01-01")
        end_date = start_date.end_of_year

        return DateRange(start_date, end_date)


class YearQuarterParser:
    @staticmethod
    def parse(period):
        if "Q" not in period:
            return
        components = period.split("Q")
        quarter = int(components.last)
        year = int(components.first)
        month = (3 * (quarter - 1)) + 1
        start_date = parseDate("#{year}-#{month}-01")

        return DateRange(start_date, start_date.end_of_quarter)


class YearMonthParser:
    @staticmethod
    def parse(period):
        if len(period) != 6:
            return
        year = period[0:3]
        month = int(period[4:5])
        start_date = parseDate("#{year}-#{month}-01")
        end_date = start_date.end_of_month

        return DateRange(start_date, end_date)


PARSERS = [YearParser, YearQuarterParser,
           YearMonthParser]


class Periods:
    @staticmethod
    def split(period, frequency):
        date_range = Periods.as_date_range(period)
        return CLASSES_MAPPING[frequency].new(date_range)

    @staticmethod
    def as_date_range(period):
        dateRange = None
        for parser in PARSERS:
            dateRange = parser.parse(period)
