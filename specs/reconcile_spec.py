import pandas as pd
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
        #print(raw_config)

        print(" ************************ input ")
        print(df)
        reconciled_data = blsqpy.reconcile.json_reconciliation(raw_config, df, reconcile_only=True)
        print(" ************************ outputs ")
        #for orgunit in ["ADlqHWi5XoF","G93DKvb6AoO","Lfne3gUweGX"]:
        #    print(reconciled_data['pills']['stock']['data_element_dict'][orgunit]['moh'])
        #    print(reconciled_data['pills']['stock']['data_element_dict'][orgunit]['ngo'])
        print(reconciled_data['pills']['stock'])

        #print(reconciled_data)


