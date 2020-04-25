import geopandas as gpd
import os
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import folium
import json
from shapely.geometry import Point

dirname = os.path.dirname(__file__)
filename = '/SPH_SHP_WGS84/WGS84/SPH_OKRES.shp'
file_loc = dirname + filename

gdf = gpd.read_file(file_loc, encoding = 'windows-1250')
gdf.NAZEV_LAU1 = gdf.NAZEV_LAU1.astype(str)
gdf.crs = {'init' :'epsg:4326'}

df = pd.read_csv('prodeje_graphs.csv')
df = df.drop(columns=['lokace'])
df = df.rename(columns={'podlazi_cislo' : 'podlazi', 'uzitnaPlocha_cislo' : 'uzitnaPlocha', 'balkon_anone' : 'balkon', 'sklep_anone' : 'sklep', 'cena_Kc' : 'cena'})
df = df.dropna(subset=['uzitnaPlocha'])

df = df[df['cena'] < 20000000]
df = df[df['cena'] > 120000]
df = df[df['stavObjektu'] != 'Projekt']
df = df[df['month_num'] != 1]

#omezeni long/lat extremnich hodnot
df = df[(df.lat > 48.5) & (df.lat < 51)]
df = df[(df.long > 10) & (df.long < 20)]

#cena na m2
df['cena_m2'] = df['cena']/df['uzitnaPlocha']
df.lat = df.lat.astype(float)
df.long = df.long.astype(float)

geom = df.apply(lambda x : Point([x['long'],x['lat']]), axis=1)
zips = gpd.GeoDataFrame(df, geometry=geom)
zips.crs = {'init' :'epsg:4326'}

joined = gpd.sjoin(zips, gdf, op='within')


okres_stats = joined.groupby(['NAZEV_LAU1','month_num'], as_index = False).agg({'cena':['count','mean'], 'cena_m2':['mean']})
okres_stats.columns = ['_'.join(x) for x in okres_stats.columns.ravel()]
okres_stats = okres_stats.rename(columns={'NAZEV_LAU1_' : 'NAZEV_LAU1', 'month_num_' : 'month_num'})

merged_areas = gdf.merge(okres_stats, on='NAZEV_LAU1', how='outer')

merged_areas['geometry'] = merged_areas['geometry'].simplify(0.001, preserve_topology=True)

#Název Plzen delal problemy s kodovanim, proto je to toto zjednodušení!
merged_areas['NAZEV_LAU1'] = merged_areas['NAZEV_LAU1'].replace({'Plzeň' : 'Plzen'}, regex=True)

merged_areas.to_file("output/prodeje_okresy.geojson", driver='GeoJSON', encoding='utf-8')

with open('output/prodeje_okresy.geojson', encoding='utf-8') as f:
    geojson_countries = json.load(f, encoding='utf-8')
    
iterations = df.month_num.unique()

for i in iterations:
    
    print(i)
    
    dct = geojson_countries.copy()
    dct["features"] = [item for item in dct["features"] 
                       if item["properties"]["month_num"] == i]
    
    copy_merged_areas = merged_areas[merged_areas['month_num'] == i]
    
    map_choropleth = folium.Map(location=[49.724,15.534],tiles='cartodbpositron', zoom_start=8, min_zoom=8, max_zoom=8, zoom_control=False)
    choropleth = folium.Choropleth(geo_data = dct,
                      data = copy_merged_areas,
                      columns=['NAZEV_LAU1', 'cena_m2_mean'],
                      key_on='properties.NAZEV_LAU1',
                      fill_color='YlGn',
                      fill_opacity=0.85,
                      line_opacity=0.2,
                      legend_name='Cena za m2 v Kč'
                      ).add_to(map_choropleth)
    
    choropleth.geojson.add_child(folium.features.GeoJsonTooltip(
            fields=['NAZEV_LAU1','cena_mean', 'cena_m2_mean'],
            aliases=['Název okresu', 'Průměrná cena [Kč]', 'Průměrná cena za m2 [Kč]'],
            style=('background-color: grey; color: white;'),
            localize=True
            )
    )
    
    map_choropleth.save(f'output/prodeje_okresy_{i}.html')
    
os.remove("output/prodeje_okresy.geojson")

okres_stats = okres_stats.rename(columns={"NAZEV_LAU1": "Název okresu"})
tables = okres_stats.pivot(index='Název okresu', columns='month_num', values='cena_m2_mean')
tables = tables.astype(int)
tables = tables.rename(columns={2: "Únor", 3: "Březen", 4: "Duben"}, errors="raise")

tables.to_csv('output/table_okresy.csv')