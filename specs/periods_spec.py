from mamba import description, context, it
from expects import expect, equal
from blsqpy.periods import Periods

with description('dhis2 periods') as self:
    with it('converts month to quarter'):
        expect(Periods.split("201601", "quarterly")).to(["2016Q1"])
        expect(Periods.split("201602", "quarterly")).to(["2016Q1"])
        expect(Periods.split("201603", "quarterly")).to(["2016Q1"])
