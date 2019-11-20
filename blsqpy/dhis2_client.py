
import requests
import pandas as pd
import geopandas
from .levels import Levels
from .geometry import geometrify
from .dot import Dot


class Dhis2Client(object):
    def __init__(self, baseurl):
        if baseurl.startswith('http'):
            self.baseurl = baseurl
        else:
            self.baseurl = Dot.load_env(baseurl)["url"]
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

    def get_geodataframe(self, geometry_type=None):
        filters = []
        if geometry_type == "point":
            filters.extend("featureType:eq:POINT")
        elif geometry_type == "shape":
            filters.extend("featureType:in:[POLYGON,MULTI_POLYGON]")
        elif geometry_type == None:
            pass
        else:
            raise Exception("unsupported geometry type")

        orgunits = self.get("organisationUnits",
                            {
                                "fields": "id,name,featureType,coordinates,level",
                                "filter": "".join(filters),
                                "paging":"false"
                            }
                            )["organisationUnits"]

        geometrify(orgunits)

        df = pd.DataFrame(orgunits)

        gdf = geopandas.GeoDataFrame(df)
        return gdf
