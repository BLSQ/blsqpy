import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import json
from blsqpy.dhis2_client import Dhis2Client
from blsqpy.geometry import geometrify
from blsqpy.dhis2 import Dhis2
from blsqpy.postgres_hook import PostgresHook
#client = Dhis2Client("https://admin:district@play.dhis2.org/2.30")
client = Dhis2Client("play-2.30.txt") 
#client=Dhis2(PostgresHook("cr_replica.txt"))


gdf=client.get_geodataframe(geometry_type="point")

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf.plot(ax=ax, color='red')
plt.show()


gdf=client.get_geodataframe(geometry_type="shape")

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf.plot(ax=ax, cmap='OrRd')
plt.show()

df = client.organisation_units_structure()
print(df)
print(df.columns)