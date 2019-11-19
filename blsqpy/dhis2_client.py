
import requests
import pandas as pd
import geopandas
from .levels import Levels
from .geometry import geometrify



class Dhis2Client(object):
    def __init__(self, baseurl):
        self.baseurl = baseurl
        self.session = requests.Session()

    def get(self, path, params=None):
        url = self.baseurl+"/api/"+path
        resp = self.session.get(url, params=params)
        print(resp.request.path_url)
        return resp.json()

    def organisation_units_structure(self):
        orgunits = []
        fields = ["id", "name", "path", "contactPerson", "memberCount",
                  "featureType", "coordinates", "closedDate", "phoneNumber", "memberCount"]
        resp = self.get("organisationUnits",
                        {"fields": ",".join(fields),
                         "paging": "false"
                         })
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
            start=1, end_offset=1, with_level=True
        )
        return orgunits_df

    def get_geodataframe(self):
        orgunits = self.get("organisationUnits",
                      {
                          "fields": "id,name,featureType,coordinates,level",
                          "filter": [
                              "featureType:!eq:POINT",
                              "level:eq:3"
                          ]
                      }
                      )["organisationUnits"]

        geometrify(orgunits)

        df = pd.DataFrame(orgunits)

        gdf = geopandas.GeoDataFrame(df)
        return gdf