########################################################
##     CREATE POINT FILE GEODATABASE FROM AIS         ##   
##    DATA FOR SOUTHFORK WINDFARM CONSTRUCTION        ##
########################################################

import pandas as pd
import geopandas as gpd
from pathlib import Path
import requests
from shapely.prepared import prep
import matplotlib.pyplot as plt
import re
import sys

# Set backend to 'Agg' to prevent plot windows from opening
plt.switch_backend('Agg')

# CONFIGURATION
base_path = Path.cwd()
csv_folder = base_path / "data-raw" / "south_fork"
output_folder = base_path / "data" / "south_fork"
gdb_path = output_folder / "south_fork_vessel_ais.gdb"
merged_layer_name = "south_fork_vessel_merged"

rest_url = "https://services7.arcgis.com/G5Ma95RzqJRPKsWL/ArcGIS/rest/services/BOEM_Renewable_States/FeatureServer/0/query?where=1=1&outFields=*&f=geojson"

output_folder.mkdir(parents=True, exist_ok=True)
hist_folder = output_folder / "vessel_histograms"
hist_folder.mkdir(parents=True, exist_ok=True)

def get_land_mask(url, buffer_meters=-200):
    print("Fetching land boundaries from REST API...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=3000)
        response.raise_for_status()
        data = response.json()
        states = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
        states_m = states.to_crs(epsg=32618)
        print(f"Applying {buffer_meters}m buffer...")
        states_m['geometry'] = states_m.buffer(buffer_meters)
        return states_m.to_crs(epsg=4326).union_all()
    except Exception as e:
        print(f"CRITICAL ERROR fetching land data: {e}")
        return None

def process_ais_to_gdb():
    land_geometry_raw = get_land_mask(rest_url)
    if land_geometry_raw is None:
        print("\n[!] Execution stopped: Land mask could not be created.")
        sys.exit()

    land_geometry = prep(land_geometry_raw)
    csv_files = list(csv_folder.glob("*.csv"))
    print(f"Found {len(csv_files)} vessels to process.")
    
    first_vessel = True 
    all_vessel_stats = {} 

    for csv_file in csv_files:
        mmsi_match = re.search(r'\d{9}', csv_file.stem)
        mmsi_label = mmsi_match.group(0) if mmsi_match else csv_file.stem
        
        print(f"Starting: {mmsi_label}...", end=" ", flush=True)
        try:
            df = pd.read_csv(csv_file, parse_dates=['BaseDateTime'])
            df.columns = df.columns.str.upper()
            df = df.dropna(subset=['MMSI', 'SOG', 'LAT', 'LON'])
            df = df[df['SOG'] <= 40]
            
            if df.empty: 
                print("Skipped (Empty)")
                continue

            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LON, df.LAT), crs="EPSG:4326")
            gdf = gdf[~gdf.geometry.apply(lambda x: land_geometry.contains(x))].copy()

            if gdf.empty:
                print("Skipped (Land)")
                continue

            # --- Time Gaps & Stats ---
            gdf = gdf.sort_values(['MMSI', 'BASEDATETIME'])
            gdf['TIME_DIFF_HOURS'] = gdf.groupby('MMSI')['BASEDATETIME'].diff().dt.total_seconds() / 3600
            
            total_pts = len(gdf)
            gaps_gt_1 = (gdf['TIME_DIFF_HOURS'] > 1).sum()
            gaps_gt_8 = (gdf['TIME_DIFF_HOURS'] > 8).sum()
            pct_gt_1 = (gaps_gt_1 / total_pts) * 100
            max_hr = gdf['TIME_DIFF_HOURS'].max()
            avg_min = gdf['TIME_DIFF_HOURS'].mean() * 60
            med_min = gdf['TIME_DIFF_HOURS'].median() * 60
            
            all_vessel_stats[mmsi_label] = {
                'MMSI': mmsi_label,
                'total_points': total_pts,
                'gaps_gt_1hr': gaps_gt_1,
                'gaps_gt_8hr': gaps_gt_8,
                'percent_gt_1hr': round(pct_gt_1, 2),
                'max_gap_hours': round(max_hr, 2),
                'avg_gap_minutes': round(avg_min, 2),
                'median_gap_minutes': round(med_min, 2)
            }

            # --- Histogram Logic (90th Percentile in Minutes) ---
            plot_data_mins = gdf['TIME_DIFF_HOURS'].dropna() * 60
            if not plot_data_mins.empty:
                p90_mins = plot_data_mins.quantile(0.90)
                upper_limit = max(p90_mins, 1.0)
                
                filtered_plot = plot_data_mins[plot_data_mins <= upper_limit]

                plt.figure(figsize=(8, 5))
                plt.hist(filtered_plot, bins=40, color='teal', edgecolor='white')
                plt.title(f"MMSI: {mmsi_label}\n(90% Gaps | Max: {upper_limit:.2f} mins)")
                plt.xlabel("Time Difference (Minutes)")
                plt.ylabel("Frequency")
                plt.grid(axis='y', alpha=0.3)
                plt.savefig(hist_folder / f"{mmsi_label}_hist.png")
                plt.close()

            # --- Export to GDB ---
            layer_name = f"V{mmsi_label}"
            
            # Individual Layer: Always 'w' to overwrite just this vessel's layer
            gdf.to_file(str(gdb_path), layer=layer_name, driver="OpenFileGDB", engine="pyogrio", mode='w')

            # Merged Layer: 'w' for the very first vessel, 'a' for all others
            if first_vessel:
                gdf.to_file(str(gdb_path), layer=merged_layer_name, driver="OpenFileGDB", engine="pyogrio", mode='w')
                first_vessel = False
            else:
                gdf.to_file(str(gdb_path), layer=merged_layer_name, driver="OpenFileGDB", engine="pyogrio", mode='a')
            
            print("Done.")

        except Exception as e:
            print(f"\nError processing {mmsi_label}: {e}")

    return all_vessel_stats

if __name__ == "__main__":
    vessel_stats = process_ais_to_gdb()
    
    if vessel_stats:
        summary_df = pd.DataFrame.from_dict(vessel_stats, orient='index')
        cols = ['MMSI', 'total_points', 'gaps_gt_1hr', 'gaps_gt_8hr', 
                'percent_gt_1hr', 'max_gap_hours', 'avg_gap_minutes', 'median_gap_minutes']
        summary_df = summary_df[cols]
        summary_path = output_folder / "vessel_timediff_summary.csv"
        summary_df.sort_values('percent_gt_1hr', ascending=False).to_csv(summary_path, index=False)
        print(f"\nProcessing Complete. Summary saved to: {summary_path}")