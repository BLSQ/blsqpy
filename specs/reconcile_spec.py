import pandas as pd
import numpy as np
import json
import blsqpy.reconcile

from mamba import description, context, it
from expects import expect, equal
from datetime import date


with description("dhis2 split") as self:

    with it("converts month to quarter"):
        df = pd.read_csv("./specs/reconcile/input.csv", sep=',')
        raw_config = None
        with open("./specs/reconcile/config.json") as f:
            raw_config = json.loads(f.read())
        # print(raw_config)

        print(" ************************ input ")
        print(df)
        reconciled_data = blsqpy.reconcile.json_reconciliation(
            raw_config, df, reconcile_only=True)
        print(" ************************ outputs ")
        reconcile = reconciled_data['pills']['stock']
        print(reconciled_data['pills']['stock'])

        print(" ************************ expected outputs ")
        expected_reconcile = pd.read_csv("./specs/reconcile/expected.csv", sep=',', dtype={
            "orgunit": object
            #np.str, monthly, pills_stock, pills_stock_source
        })
        print(expected_reconcile)

        pd.testing.assert_frame_equal(
            reconcile.reset_index( drop=True),
            expected_reconcile.reset_index( drop=True),
            check_dtype=False,
            check_index_type=False,
        )
