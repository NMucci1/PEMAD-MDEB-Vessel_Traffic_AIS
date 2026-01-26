########################################################
##     CREATE TIME-WEIGHTED DENSITY HEXBINS FROM      ##
##     AIS DATA FOR SOUTHFORK WINDFARM CONSTRUCTION   ##
########################################################

# IMPORT LIBRARIES
import geopandas as gpd
import pandas as pd
from pathlib import Path
import h3
from shapely.geometry import Polygon

# CONFIGURATION
# Set folder paths
base_path = Path.cwd()
output_folder = base_path / "data" / "south_fork"
gdb_path = output_folder / "south_fork_vessel_ais.gdb"
merged_layer_name = "south_fork_vessel_merged"

def create_detailed_hexbins(gdb_path, input_layer=merged_layer_name, resolution=8):
    print(f"Loading merged data from {input_layer}...")
    gdf = gpd.read_file(str(gdb_path), layer=input_layer, engine="pyogrio")
    
    if gdf.empty:
        return

    print(f"Generating H3 IDs at Resolution {resolution}...")
    # H3 v4.x syntax
    gdf['h3_id'] = gdf.apply(lambda row: h3.latlng_to_cell(row.geometry.y, row.geometry.x, resolution), axis=1)

    # Group by BOTH Hexagon and Vessel.
    # This keeps each vessel's time separate within the same hex.
    print("Aggregating by Hexagon + Vessel...")
    hex_summary = gdf.groupby(['h3_id', 'MMSI']).agg({
        'TIME_DIFF_HOURS': 'sum'
    }).reset_index()

    # Rename columns for clarity
    hex_summary = hex_summary.rename(columns={'TIME_DIFF_HOURS': 'VESSEL_HOURS'})

    # Create Polygon Geometries
    def h3_to_polygon(h3_id):
        boundary = h3.cell_to_boundary(h3_id)
        return Polygon([(lon, lat) for lat, lon in boundary])

    print("Generating geometries...")
    hex_summary['geometry'] = hex_summary['h3_id'].apply(h3_to_polygon)
    
    hex_gdf = gpd.GeoDataFrame(hex_summary, geometry='geometry', crs="EPSG:4326")
    
    # Save to GDB
    layer_name = f"hexbins_res{resolution}_by_vessel"
    hex_gdf.to_file(str(gdb_path), layer=layer_name, driver="OpenFileGDB", engine="pyogrio")
    print(f"Success! Layer '{layer_name}' created.")


# Run function
create_detailed_hexbins(gdb_path)