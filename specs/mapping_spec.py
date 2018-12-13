import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.descriptor import Descriptor
import blsqpy.mapping as mapping
from pandas.testing import assert_frame_equal

with description('mapping') as self:
    with it('map'):
        config = Descriptor.load("./specs/config/sample")

        expect(mapping.to_expressions(config.activities.pills)).to(
            equal(["2016Q1"]))
