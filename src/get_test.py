import logging
import geopandas as gpd
import vridownload
from shapely.geometry import shape
import time
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch.setFormatter(formatter)

#call VRI downloader
wfs = vridownload.WFS_downloader()

#Get AOI and bbox
start = time.time()
logger.debug("Starting to get data... ")
aoi=wfs.get_data(dataset='WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP', query="UWR_NUMBER = 'u-4-001'")
dtime = time.time() - start
logger.debug(f'process took {round(dtime)} seconds')
#geojson to gdf
logger.debug(f'Writing {len(aoi)} features to geodataframe')
start = time.time()
df = wfs.features_to_df(aoi)
dtime = time.time() - start
logger.debug(f'process took {round(dtime)} seconds')
#gdf dissolve get bounding box 
df=df.dissolve()
aoi_bounds= df.total_bounds.tolist()
bbox=['%.0f' % elem for elem in aoi_bounds]
bbox=[int(x) for x in bbox]
bbox.append("urn:ogc:def:crs:EPSG:3005")
bbox=tuple(bbox)
print(bbox)

#====this bbox works====
# bbox= (1540213, 476326, 1748418, 550000,"urn:ogc:def:crs:EPSG:3005")
# print(bbox)

start = time.time()
logger.debug("Starting to get data... ")
response = wfs.get_data(dataset='WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY', bbox=bbox)
dtime = time.time() - start
logger.debug(f'process took {round(dtime)} seconds')
#geojson to gdf
logger.debug(f'Writing {len(response)} features to geodataframe')
start = time.time()
df = wfs.features_to_df(response)
dtime = time.time() - start
logger.debug(f'process took {round(dtime)} seconds')
#VRI to geoparquet 
df.to_parquet(r'VRI')


