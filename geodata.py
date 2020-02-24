import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import json
from blsqpy.dhis2_client import Dhis2Client
from blsqpy.geometry import geometrify
from blsqpy.dhis2 import Dhis2
from blsqpy.postgres_hook import PostgresHook
client1 = Dhis2Client("https://admin:district@play.dhis2.org/2.32.3")
df=client1.data_elements_structure()
client2=Dhis2(PostgresHook("cr_replica.txt"))
df2=client2.data_elements_structure()
print( df.columns)
print( df2.columns)
b = True
if b:
  exit() 

#client2 = Dhis2Client("play-2.30.txt") 
client2=Dhis2(PostgresHook("cr_replica.txt"))

gdf1=client1.get_geodataframe(geometry_type=None)
gdf2=client2.get_geodataframe(geometry_type="point")

def as_type(x):
        return x['geometry'].__class__.__name__
gdf1['geometry_type'] = gdf1.apply((lambda x: as_type(x)), axis=1)

print(gdf1['geometry_type'].unique())
print(gdf1.columns)

#print(gdf2)
print(gdf2.columns)

df1 = client1.organisation_units_structure()
df2 = client2.organisation_units_structure()

print(df1.columns)
print(df2.columns)

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf1.plot(ax=ax, color='red')
gdf2.plot(ax=ax, color='red')
plt.show()


gdf1=client1.get_geodataframe(geometry_type="shape")
gdf2=client2.get_geodataframe(geometry_type="shape")

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
ax = world[world.continent == 'Africa'].plot(
    color='white', edgecolor='black')
gdf1.plot(ax=ax, cmap='OrRd')
gdf2.plot(ax=ax, cmap='OrRd')
plt.show()

