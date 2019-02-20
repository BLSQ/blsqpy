import pandas as pd
from mamba import description, context, it
from expects import expect, equal
from blsqpy.periods import Periods
from datetime import date

from blsqpy.query import get_query

with description("query") as self:

    with it("loads and render the correct query template and append query name as comment"):
       sql = get_query('demo', {'table': 'demo_table'})
       expect(sql).to(equal("select * from demo_table limit 10\n -- query : demo"))