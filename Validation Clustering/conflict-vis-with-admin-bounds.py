import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

df = pd.read_csv("conflict_locations_geocoded.csv")
df = df.dropna(subset=["lat", "long"])

gdf = gpd.GeoDataFrame(
    df,
    geometry=[Point(xy) for xy in zip(df["long"], df["lat"])],
    crs="EPSG:4326"  # WGS84
)

kerala_state = gpd.read_file("kerala_state.shp")
kerala_districts = gpd.read_file("kerala_districts.shp")

# Ensure same CRS
kerala_state = kerala_state.to_crs(gdf.crs)
kerala_districts = kerala_districts.to_crs(gdf.crs)
