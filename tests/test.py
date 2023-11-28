import sys
sys.path.insert(0,'../src')

from datatools import WFS_downloader

page_size = 10000
start_index = 0
bbox_albers = (1569306,513998,1676611,631824,"urn:ogc:def:crs:EPSG:3005")
#bbox_albers = (1491867, 480179, 1567106, 566654,"urn:ogc:def:crs:EPSG:3005")
wfs = WFS_downloader()
response = wfs.get_data(dataset='WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY',bbox=bbox_albers)
print (response)