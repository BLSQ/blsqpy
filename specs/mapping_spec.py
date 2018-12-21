import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.descriptor import Descriptor
import blsqpy.mapping as mapping
from pandas.testing import assert_frame_equal
from collections import OrderedDict

config = Descriptor.load("./specs/fixtures/config/sample")


with description('mapping') as self:
    with it('build mapping when multiple uuid'):
        expect(mapping.to_mappings(config.activities.pills, "pills")).to(
            equal(OrderedDict([
                ('s4CxsmoqdRj.oFtySBIkit2', 'pills_active_moh'),
                ('fSD1ZZo4hTs.oFtySBIkit2', 'pills_delivered_1_moh'),
                ('p4GY25ry19H.pQqm0IZ6AJ3', 'pills_delivered_2_moh'),
                ('sur5ZhRRQYp.HllvX50cXC0', 'pills_delivered_3_moh'),
                ('s4CxsmoqdRj.Vkfh73dsruW', 'pills_new_program_moh'),
                ('s4CxsmoqdRj.pQqm0IZ6AJ3', 'pills_new_structure_moh')])))

    with it('build expressions when multiple uuid'):
        expect(mapping.to_expressions(config.activities.pills, "pills")).to(
            equal({'pills_delivered_moh': [
                'pills_delivered_1_moh',
                'pills_delivered_2_moh',
                'pills_delivered_3_moh'
            ]}))

    with it('maps a dataframe with from coc.de to activity_state_source columns'):
        df = pd.read_csv("./specs/fixtures/extract/rotated.csv", sep=',')
        mapped_df = mapping.map_from_activity(
            df, config.activities.pills, "pills")
        print(df)
        print("*************** mapped_df")
        print(mapped_df)

        #mapped_df.to_csv("./specs/mapping/mapped.csv", sep=',')
        expected_mapped = pd.read_csv("./specs/fixtures/mapping/mapped.csv", sep=',')
        print("*************** expected_mapped")
        print(expected_mapped)

        assert_frame_equal(
            mapped_df.reset_index(drop=True),
            expected_mapped,
            check_dtype=False)
