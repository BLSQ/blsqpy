from mamba import description, context, it
from expects import expect, equal
from blsqpy.periods import Periods
from datetime import date

with description("dhis2 split") as self:
    with it("converts month to quarter"):
        expect(Periods.split("201601", "quarterly")).to(equal(["2016Q1"]))
        expect(Periods.split("201602", "quarterly")).to(equal(["2016Q1"]))
        expect(Periods.split("201603", "quarterly")).to(equal(["2016Q1"]))

    with it("converts month to year"):
        expect(Periods.split("201601", "yearly")).to(equal(
            ["2016"]))

    with it("converts quarter to months"):
        expect(Periods.split("2016Q1", "monthly")).to(equal(
            ["201601", "201602", "201603"]))
        expect(Periods.split("2016Q2", "monthly")).to(
            equal(["201604", "201605", "201606"]))
        expect(Periods.split("2016Q3", "monthly")).to(
            equal(["201607", "201608", "201609"]))
        expect(Periods.split("2016Q4", "monthly")).to(
            equal(["201610", "201611", "201612"]))

    with it("converts quarter to year"):
        expect(Periods.split("2016Q1", "yearly")).to(equal(
            ["2016"]))

    with it("converts year to quarter"):
        expect(Periods.split("2016", "quarterly")).to(equal(
            ["2016Q1", "2016Q2", "2016Q3", "2016Q4"]))

    with it("converts year to months"):
        expect(Periods.split("2016", "monthly")).to(equal(
            ['201601', '201602', '201603', '201604', '201605', '201606', '201607', '201608', '201609', '201610', '201611', '201612']))

    with it("cached and non cached version returns same type"):
        first = Periods.split("2018", "monthly")
        last = Periods.split("2018", "monthly")
        expect(first).to(equal(last))

with description("as_date_range") as self:
    with it("for monthly"):
        expect(Periods.as_date_range("201601").start).to(equal(date(2016,1,1)))
        expect(Periods.as_date_range("201601").end).to(equal(date(2016,1,31)))

    with it("for quarter"):
        expect(Periods.as_date_range("2016Q3").start).to(equal(date(2016,7,1)))
        expect(Periods.as_date_range("2016Q3").end).to(equal(date(2016,9,30)))
        expect(Periods.as_date_range("2016Q4").start).to(equal(date(2016,10,1)))
        expect(Periods.as_date_range("2016Q4").end).to(equal(date(2016,12,31)))

    with it("for year"):
        expect(Periods.as_date_range("2016").start).to(equal(date(2016,1,1)))
        expect(Periods.as_date_range("2016").end).to(equal(date(2016,12,31)))
