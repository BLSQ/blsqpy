import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.descriptor import Descriptor
import blsqpy.mapping as mapping
from pandas.testing import assert_frame_equal

with description('mapping') as self:

    with it('build mapping when multiple uuid'):
        config = Descriptor.load("./specs/config/sample")

        expect(mapping.to_mappings(config.activities.pills, "pills")).to(
            equal({
                'sur5ZhRRQYp.HllvX50cXC0': 'pills_delivered_3_moh',
                'p4GY25ry19H.HllvX50cXC0': 'pills_delivered_2_moh',
                's4CxsmoqdRj.pQqm0IZ6AJ3': 'pills_new_structure_moh',
                's4CxsmoqdRj.Vkfh73dsruW': 'pills_new_program_moh',
                's4CxsmoqdRj.oFtySBIkit2': 'pills_active_moh',
                'fSD1ZZo4hTs.HllvX50cXC0': 'pills_delivered_1_moh'}))

    with it('build expressions when multiple uuid'):
        config = Descriptor.load("./specs/config/sample")

        expect(mapping.to_expressions(config.activities.pills, "pills")).to(
            equal({'pills_delivered_moh': [
                'pills_delivered_1_moh',
                'pills_delivered_2_moh',
                'pills_delivered_3_moh'
            ]}))
