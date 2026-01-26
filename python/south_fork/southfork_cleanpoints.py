########################################################
##     CREATE POINT FILE GEODATABASE FROM AIS         ##   
##    DATA FOR SOUTHFORK WINDFARM CONSTRUCTION        ##
########################################################

# IMPORT LIBRARIES
import pandas as pd
import geopandas as gpd
from pathlib import Path
import requests
from shapely.prepared import prep

# CONFIGURATION
# Set folder paths
base_path = Path.cwd()
csv_folder = base_path / "data-raw" / "south_fork"
output_folder = base_path / "data" / "south_fork"
gdb_path = output_folder / "south_fork_vessel_ais.gdb"
merged_layer_name = "south_fork_vessel_merged"
# BOEM Renewable States REST URL for land masking
rest_url = "https://services7.arcgis.com/G5Ma95RzqJRPKsWL/ArcGIS/rest/services/BOEM_Renewable_States/FeatureServer/0/query?where=1=1&outFields=*&f=geojson"

# Ensure output directory exists
output_folder.mkdir(parents=True, exist_ok=True)

# Function to create a coastal land mass area
# This is used to remove AIS pings that occur on land
# Use -200 m buffer to ensure AIS pings in harbor aren't removed
def get_land_mask(url, buffer_meters=-200):
    print("Fetching land boundaries from REST API...")
    
    # Use requests to pull the data with a timeout and a 'browser-like' header
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Check for errors
        data = response.json()
        
        # Load the JSON data into a GeoDataFrame
        states = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
        print("Data received successfully.")
        
    except Exception as e:
        print(f"Failed to fetch data: {e}")
        return None

    # Project to UTM 18N (meters) for accurate buffering
    states_m = states.to_crs(epsg=32618)
    print(f"Applying {buffer_meters}m buffer...")
    states_m['geometry'] = states_m.buffer(buffer_meters)
    
    return states_m.to_crs(epsg=4326).union_all()

# Function to clean AIS data and create point feature classes
def process_ais_to_gdb():
    # Prepare the spatial land filter
    land_geometry_raw = get_land_mask(rest_url)
    
    print("Optimizing land geometry for fast processing...")
    land_geometry = prep(land_geometry_raw)
    
    # Find all vessel CSVs
    csv_files = list(csv_folder.glob("*.csv"))
    print(f"Found {len(csv_files)} vessels to process.")
    
    first_vessel = True 

    for csv_file in csv_files:
        print(f"Starting: {csv_file.name}...", end=" ", flush=True)
        try:
            # Load Data
            df = pd.read_csv(csv_file, parse_dates=['BaseDateTime'])
            df.columns = df.columns.str.upper()
            
            # Attribute Filters
            df = df.dropna(subset=['MMSI', 'SOG', 'LAT', 'LON'])
            df = df[df['SOG'] <= 40]
            if df.empty: 
                print("Skipped (Empty after SOG filter)")
                continue

            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df.LON, df.LAT), 
                crs="EPSG:4326"
            )
            
            # Spatial Filter using the PREPARED geometry
            gdf = gdf[~gdf.geometry.apply(lambda x: land_geometry.contains(x))].copy()

            if gdf.empty:
                print("Skipped (All points on land)")
                continue

            # Time Gaps
            gdf = gdf.sort_values('BASEDATETIME')
            gdf['TIME_DIFF_HOURS'] = gdf['BASEDATETIME'].diff().dt.total_seconds() / 3600
            gdf['FLAG_TIME_GAP'] = (gdf['TIME_DIFF_HOURS'] > 4).astype(int)

            # Export
            clean_name = f"{csv_file.stem.replace('.', '_').replace('-', '_')}"
            
            # Individual Layer
            gdf.to_file(str(gdb_path), layer=clean_name, driver="OpenFileGDB", engine="pyogrio")

            # Merged Layer
            if first_vessel:
                gdf.to_file(str(gdb_path), layer=merged_layer_name, driver="OpenFileGDB", engine="pyogrio")
                first_vessel = False
            else:
                gdf.to_file(str(gdb_path), layer=merged_layer_name, driver="OpenFileGDB", engine="pyogrio", mode="a")
            
            print("Done.")

        except Exception as e:
            print(f"\nError processing {csv_file.name}: {e}")

if __name__ == "__main__":
    process_ais_to_gdb()
    print("\nProcessing Complete. Data is located in:")
    print(gdb_path)


