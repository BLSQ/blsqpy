import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import json
from blsqpy.dhis2_client import Dhis2Client
from shapely.geometry import LineString, Polygon, MultiPolygon, Point, shape


def as_geometry(coordinates):
    x = json.loads(coordinates)
    if coordinates.startswith("[[[["):
        geojson = {"type": "MultiPolygon", "coordinates": x}
        return shape(geojson)
    if coordinates.startswith("[[["):
        geojson = {"type": "Polygon", "coordinates": x}
        return shape(geojson)
    if coordinates.startswith("[["):
        return LineString(x)
    if coordinates.startswith("["):
        return Point(float(x[0]), float(x[1]))


def geometrify(orgunits):
    for orgunit in orgunits:
        if "coordinates" in orgunit:
            orgunit["geometry"] = as_geometry(orgunit["coordinates"])


client = Dhis2Client("https://admin:district@play.dhis2.org/2.30")

orgunits = client.get("organisationUnits",
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
world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf.plot(ax=ax, color='red')
plt.show()


df = pd.DataFrame(orgunits)

orgunits = client.get("organisationUnits",
                      {
                          "fields": "id,name,featureType,coordinates,level",
                          "filter": "featureType:eq:POINT",
                          "paging": "false"
                      }
                      )["organisationUnits"]

geometrify(orgunits)

df = pd.DataFrame(orgunits)
gdf = geopandas.GeoDataFrame(df)
world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf.plot(ax=ax, color='red')
plt.show()

print(client.organisation_units_structure())