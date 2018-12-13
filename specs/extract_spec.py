import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.descriptor import Descriptor
import blsqpy.extract as extract
from pandas.testing import assert_frame_equal

with description('extract') as self:

    with it('extract data elements from config'):
        config = Descriptor.load("./specs/config/sample")

        data_elements = extract.to_data_elements(config.activities.pills)
        expect(data_elements).to(equal([
            's4CxsmoqdRj.oFtySBIkit2',
            'fSD1ZZo4hTs.oFtySBIkit2',
            'p4GY25ry19H.pQqm0IZ6AJ3',
            'sur5ZhRRQYp.HllvX50cXC0',
            's4CxsmoqdRj.Vkfh73dsruW',
            's4CxsmoqdRj.pQqm0IZ6AJ3']
        ))

    with it("rotate data_element.category_option_combo as columns"):
        df = pd.read_csv("./specs/extract/raw.csv", sep=',')
        df_rotated = extract.rotate_de_coc_as_columns(df)
        # df_rotated.to_csv("./specs/extract/rotated.csv")
        expected_rotated = pd.read_csv("./specs/extract/rotated.csv", sep=',')
        expected_rotated.set_index(['period', 'orgunit'], inplace=True)
        print(expected_rotated)
        print(df_rotated)

        assert_frame_equal(
            df_rotated.reset_index(),
            expected_rotated.reset_index(),
            check_dtype=False, check_names=False
        )
