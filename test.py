
import code

from blsqpy.s3export_hooks import S3ExportsHook
from blsqpy.postgres_hook import PostgresHook
from blsqpy.dhis2 import Dhis2

import numpy as np
import pandas as pd
import code

# Sample config usage

from blsqpy.descriptor import Descriptor
config = Descriptor.load("./config/demo")
print("hello", config.demo.test.hello)


# S3  : listing and reading
s3 = S3ExportsHook("s3_readonly")
exports = s3.exports()

print("Available extracts")
for val in exports:
    print(val["Key"], "\t", val["Size"], "\t", str(val["LastModified"]))

# S3 get a specific file and use it as a dataframe

s3 = S3ExportsHook("s3_readonly")
s3.download_file(
    "export/datavalues_extract_data_values_dhis2_sn_cartesanitaire_pilule.csv", "./tmp/pilule.csv")
pilule = pd.read_csv("./tmp/pilule.csv", compression='gzip')
print(pilule.size)


dhis2 = Dhis2(PostgresHook("dhis2_cd_hivdr_prod"))
df = dhis2.get_data(["Yj8caUQs178.unkown"])
print("fetched data : ", df.size)

#code.interact(local=locals())
