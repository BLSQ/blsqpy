import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.descriptor import Descriptor
import blsqpy.extract as extract
from pandas.testing import assert_frame_equal

with description('mapping') as self:
    with it('map'):
        config = Descriptor.load("./specs/config/sample")