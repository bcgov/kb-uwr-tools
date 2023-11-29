import logging
import geopandas as gpd
import feature_download
from shapely.geometry import shape
import time
import numpy as np

logging.basicConfig(level=logging.INFO)

#call Feature downloader
wfs = feature_download.WFS_downloader()

#Get AOI and bbox
start = time.time()
logging.info("Starting to get data... ")

#Get data
aoi=wfs.get_data(dataset='WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP', query="UWR_NUMBER = 'u-4-001'")

#features to geopandas geodataframe
df = wfs.features_to_df(aoi)
df.to_parquet(r'UWR4001.parquet')

#get bounding box
bbox=wfs.create_bbox(df)

del df
dtime = time.time() - start
logging.info(f'process took {round(dtime)} seconds')

#get vri data for bbox
start = time.time()
logging.info("Starting to get data... ")
response = wfs.get_data(dataset='WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY', bbox=bbox)
dtime = time.time() - start
logging.info(f'process took {round(dtime)} seconds')

# vri gdf to geo-parquet
response.to_parquet(r'VRI.parquet')


