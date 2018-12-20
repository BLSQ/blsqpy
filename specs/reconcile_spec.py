import pandas as pd
import numpy as np
import json
import blsqpy.reconcile
from blsqpy.descriptor import Descriptor

from mamba import description, context, it
from expects import expect, equal
from datetime import date


with description("reconcil") as self:
    with it("reconciliate will take the prefered source"):
        config = Descriptor.load("./specs/reconcile/config")
        df = pd.read_csv("./specs/reconcile/input.csv", sep=',')
        print(" ************************ input ")
        print(df)

        reconciled_data = blsqpy.reconcile.reconciliate(config, df)

        print(" ************************ outputs ")
        reconciled_df = reconciled_data['pills']['stock']
        print(reconciled_df)

        print(" ************************ expected outputs ")
        expected_reconcile = pd.read_csv(
            "./specs/reconcile/expected.csv", sep=',')
        print(expected_reconcile)

        pd.testing.assert_frame_equal(
            reconciled_df.reset_index(drop=True),
            expected_reconcile.reset_index(drop=True),
            check_dtype=False,
        )
