import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import json
from blsqpy.dhis2_client import Dhis2Client
from blsqpy.geometry import geometrify
client = Dhis2Client("https://admin:district@play.dhis2.org/2.30")

gdf=client.get_geodataframe()

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf.plot(ax=ax, color='red')
plt.show()


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

df = client.organisation_units_structure()
print(df)
print(df.columns)