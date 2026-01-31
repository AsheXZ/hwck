import pandas as pd
from pygbif import occurrences
import geopandas as gpd
from shapely.geometry import Point

# --- CONFIGURATION ---
# Taxon Keys (GBIF IDs)
SPECIES_MAP = {
    'Elephas maximus': 2435146, # Asian Elephant
    'Panthera tigris': 5219410, # Tiger
    'Sus scrofa': 2441218,      # Wild Boar
    'Bos gaurus': 2441097       # Gaur (Indian Bison)
}

# Kerala Bounding Box (Rough approximation to speed up query)
# min_lat, min_lon, max_lat, max_lon
KERALA_BBOX = '74.8,8.1,77.5,12.8' 

def fetch_gbif_data():
    print("Querying Global Biodiversity Information Facility (GBIF)...")
    all_records = []

    for species_name, taxon_key in SPECIES_MAP.items():
        print(f"  > Fetching records for: {species_name}...")
        
        # Pagination loop (GBIF limits to 300 per request)
        offset = 0
        while True:
            results = occurrences.search(
                taxonKey=taxon_key,
                geometry=f'POLYGON((74.8 8.1, 77.5 8.1, 77.5 12.8, 74.8 12.8, 74.8 8.1))', # WKT Polygon
                hasCoordinate=True,
                limit=300,
                offset=offset,
                year='2018,2024' # Last 6 years
            )
            
            if not results['results']:
                break
                
            for rec in results['results']:
                # Filter strictly for Kerala (API polygon is rectangular, so we double check)
                state = rec.get('stateProvince', '').lower()
                if 'kerala' in state or ('tamil' not in state and 'karnataka' not in state): 
                    # (Simple string check, geofencing is better done in post-processing)
                    
                    all_records.append({
                        'event_date': rec.get('eventDate', '').split('T')[0],
                        'species': species_name,
                        'latitude': rec.get('decimalLatitude'),
                        'longitude': rec.get('decimalLongitude'),
                        'basis_of_record': rec.get('basisOfRecord'), # e.g., HUMAN_OBSERVATION
                        'source': 'GBIF/iNaturalist',
                        'raw_id': rec.get('key')
                    })
            
            offset += 300
            if results['endOfRecords']:
                break
    
    return pd.DataFrame(all_records)

# --- POST-PROCESSING ---
def filter_proxy_conflicts(df):
    """
    Clever Logic: An elephant in the forest is nature. 
    An elephant in a farm is conflict.
    
    We need to filter points that are LIKELY conflicts.
    Since we don't have the landcover loaded here, we prepare the data for that join.
    """
    # 1. Filter for "Human Observation" (exclude museum specimens/fossils)
    df = df[df['basis_of_record'] == 'HUMAN_OBSERVATION']
    
    # 2. Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    
    return gdf

if __name__ == "__main__":
    df_raw = fetch_gbif_data()
    print(f"Fetched {len(df_raw)} biological occurrences.")
    
    if not df_raw.empty:
        gdf_clean = filter_proxy_conflicts(df_raw)
        print(f"Filtered to {len(gdf_clean)} likely citizen-science sightings.")
        
        # Save
        gdf_clean.to_csv("kerala_gbif_occurrences.csv", index=False)
        print("Saved to 'kerala_gbif_occurrences.csv'.")
        print("NEXT STEP: Overlay this with ESA WorldCover. Points in Class 40 (Crop) = VALIDATION EVENTS.")