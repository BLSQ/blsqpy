
import requests
import pandas as pd
from .levels import Levels


class Dhis2Client(object):
    def __init__(self, baseurl):
        self.baseurl = baseurl

    def organisation_units_structure(self):
        orgunits = []
        resp = requests.get(
            self.baseurl+"/api/organisationUnits?fields=id,name,path&paging=false").json()

        for record in resp["organisationUnits"]:
            orgunits.append((record["id"], record["name"], record["path"]))

        orgunits_df = pd.DataFrame(orgunits)
        orgunits_df.columns=['organisationunituid', 'organisationunitname', 'path']

        Levels.add_uid_levels_columns_from_path_column(
            orgunits_df,
            start=1, end_offset=2, with_level=True
        )
        return orgunits_df
