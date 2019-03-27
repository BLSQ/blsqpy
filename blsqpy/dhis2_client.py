
import requests
import pandas as pd
from .levels import Levels


class Dhis2Client(object):
    def __init__(self, baseurl):
        self.baseurl = baseurl

    def organisation_units_structure(self):
        orgunits = []
        fields = ["id", "name", "path", "contactPerson", "memberCount",
                  "featureType", "coordinates", "closedDate", "phoneNumber", "memberCount"]
        url = self.baseurl+"/api/organisationUnits?fields="+(",".join(fields))+"&paging=false"
        print(url)
        resp = requests.get(url).json()
        for record in resp["organisationUnits"]:
            line = []
            for field in fields:
                if field in record:
                    line.append(record[field])
                else:
                    line.append(None)
            orgunits.append(line)

        orgunits_df = pd.DataFrame(orgunits)
        orgunits_df.columns = ['organisationunituid',
                               'organisationunitname', 'path', "contactPerson", "memberCount",
                               "featureType", "coordinates", "closedDate", "phoneNumber", "memberCount"]

        Levels.add_uid_levels_columns_from_path_column(
            orgunits_df,
            start=1, end_offset=2, with_level=True
        )
        return orgunits_df
