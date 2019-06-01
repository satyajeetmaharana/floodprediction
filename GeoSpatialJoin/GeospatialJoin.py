import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import Point

stations = pd.DataFrame.from_csv('stations.csv',index_col=None)

print(type(stations))


stations['Longitude'] = -1 * (stations['Longitude'] - 180)


geom = stations.apply(lambda x : Point([x['LONGITUDE'],x['LATITUDE']]),axis=1)


stations = gpd.GeoDataFrame(stations, geometry=geom) #geom is a Series


stations.crs = {'init' :'epsg:4326'}


geojson_file = 'US_Cities.geojson'


cities = gpd.read_file(geojson_file)[['NAME','geometry']]


stations.head()


cities.head()


stations_and_cities = gpd.sjoin(cities,stations,op='within')


stations_and_cities.to_csv(r'stations_withGeo.csv',index=False)
