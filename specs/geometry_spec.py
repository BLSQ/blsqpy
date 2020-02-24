import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.geometry import as_geometry

with description('geometrify') as self:
    with it('should support string float'):
        point=as_geometry('[ "1.5", "2.3"]')
        expect(point.x).to(equal(1.5))
        expect(point.y).to(equal(2.3))
    with it('should fails silently when invalid coordinates'):
        point=as_geometry('[ "a", "b"]')
        expect(point).to(equal(None))
       