########################################################
##     CREATE TRIP TRACKLINES FROM AIS DATA           ##
##    WITH STATUS TRANSITIONS AND STATIONARY FLAGS    ##
########################################################

import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point, LineString
from shapely.prepared import prep

# CONFIGURATION
# Set folder paths
base_path = Path.cwd()
gdb_path = base_path / "data" / "south_fork" / "south_fork_vessel_ais.gdb"
input_layer = "south_fork_vessel_merged"
output_layer = "south_fork_vessel_trips_lines"

# Port Coordinates for Geofencing
ports_data = {
    'ProvPort': (-71.391, 41.802), 'New_London': (-72.093, 41.354),
    'Point_Judith': (-71.488, 41.363), 'Quonset': (-71.415, 41.585),
    'Montauk': (-71.931, 41.074), 'New_Bedford': (-70.923, 41.636),
    'Bridgeport': (-73.181, 41.173), 'Shinnecock': (-72.476, 40.842),
    'Fall_River': (-71.164, 41.704), 'Fairhaven': (-70.906, 41.624),
    'Newport_RI': (-71.328, 41.484), 'Sakonnet_Harbor': (-71.193, 41.464),
    'Brooklyn_NY': (-74.015, 40.672), 'Charleston_SC': (-79.927, 32.818),
    'Corpus_Christi': (-97.395, 27.800), 'Millville_NJ': (-75.044, 39.213)
}

def run_trackline_pipeline():
    # Load the merged point layer
    print(f"Reading points from {input_layer}...")
    gdf = gpd.read_file(str(gdb_path), layer=input_layer, engine="pyogrio")
    
    # Ensure column names are standardized
    gdf.columns = gdf.columns.str.upper()
    if "GEOMETRY" in gdf.columns:
        gdf = gdf.set_geometry("GEOMETRY")
    gdf['BASEDATETIME'] = pd.to_datetime(gdf['BASEDATETIME'])
    gdf = gdf.sort_values(['MMSI', 'BASEDATETIME'])

    # Build Port Mask for Geofencing
    print("Creating port geofences...")
    port_pts = [Point(lon, lat) for lon, lat in ports_data.values()]
    ports_gdf = gpd.GeoDataFrame({'port_name': list(ports_data.keys())}, geometry=port_pts, crs="EPSG:4326")
    ports_mask_m = ports_gdf.to_crs(epsg=32618)
    ports_mask_m['geometry'] = ports_mask_m.buffer(1000) # 1km radius
    port_mask_geom = ports_mask_m.to_crs(epsg=4326).union_all()

    # Trip Segmentation/Creation & Flagging Logic
    print("Processing behavioral flags and trip segments...")
    
    # Port Status
    # Create column for whether points are within the port geofence
    gdf['IN_PORT'] = gdf.geometry.within(port_mask_geom)
    
    # Create stationary flag (SOG < 1 for > 1 hour)
    gdf['TIME_DIFF'] = gdf.groupby('MMSI')['BASEDATETIME'].diff().dt.total_seconds() / 3600
    gdf['IS_LOW_SPEED'] = gdf['SOG'] < 1.0
    gdf['state_group'] = (gdf['IS_LOW_SPEED'] != gdf['IS_LOW_SPEED'].shift()).cumsum()
    state_durations = gdf.groupby(['MMSI', 'state_group'])['TIME_DIFF'].transform('sum')
    gdf['FLAG_STATIONARY'] = ((gdf['IS_LOW_SPEED'] == True) & (state_durations >= 1.0)).astype(int)

    # Status Transitions (Status 1=Anchor, 5=Moored)
    # Create a new trip when status changes from 1 or 5 to another status
    parked_statuses = [1, 5]
    gdf['STATUS_CHANGED'] = (
        (~gdf['STATUS'].isin(parked_statuses)) & 
        (gdf['STATUS'].shift(1).isin(parked_statuses))
    )

    # Trip ID Generation
    # New trip if: Left Port OR Gap between points > 8hrs OR Status changed from Anchor/Moored
    gdf['TRIP_START'] = (
        ((gdf['IN_PORT'] == False) & (gdf['IN_PORT'].shift(1) == True)) | 
        (gdf['TIME_DIFF'] > 8) |
        (gdf['STATUS_CHANGED'] == True)
    )
    gdf['TRIP_ID'] = gdf.groupby('MMSI')['TRIP_START'].cumsum()

    # Connect Points to Lines
    print("Creating tracklines...")
    # Group and create lines
    lines_series = gdf.groupby(['MMSI', 'TRIP_ID'])['GEOMETRY'].apply(
        lambda x: LineString(x.tolist()) if len(x) >= 2 else None
    ).dropna()
    
    # Explicitly set the geometry column name in the constructor
    lines_gdf = gpd.GeoDataFrame(
        lines_series, 
        geometry='GEOMETRY', 
        crs="EPSG:4326"
    ).reset_index()

    # Rename to lowercase for GDB standard compatibility
    lines_gdf = lines_gdf.rename_geometry('geometry')

    # Calculate Trip Metrics (Duration & Distance)
    print("Calculating trip duration and distance...")
    
    # Metadata and Duration
    metrics = gdf.groupby(['MMSI', 'TRIP_ID']).agg({
        'BASEDATETIME': ['min', 'max'],
        'FLAG_STATIONARY': 'max' # Did this trip contain a stationary period?
    }).reset_index()
    metrics.columns = ['MMSI', 'TRIP_ID', 'START_TIME', 'END_TIME', 'HAD_STATIONARY']
    metrics['DURATION_HRS'] = (metrics['END_TIME'] - metrics['START_TIME']).dt.total_seconds() / 3600
    
    # Project to UTM 18N for distance (Nautical Miles)
    lines_utm = lines_gdf.to_crs(epsg=32618)
    lines_gdf['DIST_NM'] = (lines_utm.geometry.length * 0.000539957)

    # Merge everything
    final_lines = lines_gdf.merge(metrics, on=['MMSI', 'TRIP_ID'])

    # Final Export to GDB
    print(f"Saving tracklines to {output_layer}...")
    final_lines.to_file(str(gdb_path), layer=output_layer, driver="OpenFileGDB", engine="pyogrio")
    
    print("Ship trip creation complete!")

if __name__ == "__main__":
    run_trackline_pipeline()